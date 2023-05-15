package org.alfa.beam.exporter;

import com.google.common.collect.ImmutableList;

import java.sql.Timestamp;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Random;

import com.ibm.mq.jms.MQConnectionFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ExtendedJsonDecoder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.io.jms.JmsIO;
import org.apache.beam.sdk.io.jms.JmsRecord;
import org.apache.beam.sdk.io.parquet.ParquetIO;
;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.log4j.Logger;
import org.joda.time.Duration;
import org.json.JSONObject;
import org.json.XML;
import org.alfa.beam.exporter.options.JmsExportingOptions;
import java.util.Iterator;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.stream.StreamSource;
import javax.xml.transform.stream.StreamResult;
import net.sf.saxon.TransformerFactoryImpl;
import org.apache.commons.io.IOUtils;
import org.messaginghub.pooled.jms.JmsPoolConnectionFactory;

public class JmsExportingJob {

    private static class ConvertJsonToGenericRecord extends DoFn<String, GenericRecord> {

        private final String schemaJson;

        private final String keyField;

        private Schema schema;

        ConvertJsonToGenericRecord(Schema schema, String keyField){
            this.schemaJson = schema.toString();
            this.keyField   = keyField;
        }

        @Setup
        public void setup(){
            schema = new Schema.Parser().parse(schemaJson);
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            String json = c.element();
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord> (schema);
            Decoder decoder = new ExtendedJsonDecoder(schema, json);
            GenericRecord record = reader.read(null,decoder);
            if (!keyField.isEmpty() && record.get(keyField).toString().isEmpty()) {
                return;
            }
            c.output(record);
            log.warn("DEBUG WARNING:" + record + "\n" + json);
        }
    }

    private static Logger log = Logger.getLogger(JmsExportingJob.class);


    private static String decodeText(String input, Charset charset) throws IOException {
        CharsetDecoder charsetDecoder = charset.newDecoder();
        return (new BufferedReader(new InputStreamReader(new ByteArrayInputStream(input.getBytes()), charsetDecoder))).readLine();
    }

    private static class GetTargetFieldFn extends DoFn<GenericRecord, GenericRecord> {
        private final String fieldName;
        private final String targetFieldName;

        public GetTargetFieldFn(String fieldName, String targetFieldName) {
            this.fieldName = fieldName;
            this.targetFieldName = targetFieldName;
        }

        @ProcessElement
        public void processElement(DoFn<GenericRecord, GenericRecord>.ProcessContext c) throws IOException {
            GenericRecord record = (GenericRecord)c.element();
            String recordSchemaString = String.format("{\"type\":\"record\",\"name\":\"result\",\"fields\":[{\"name\":\"%s\",\"type\":\"string\"}]}", this.targetFieldName);
            Schema.Parser parser = new Schema.Parser();
            Schema recordSchema = parser.parse(recordSchemaString);
            GenericRecord result = new GenericData.Record(recordSchema);
            result.put(this.targetFieldName, record.get(this.fieldName));
            if (result != null) {
                c.output(result);
            }

        }
    }

    private static class AssignRandomKeys extends DoFn<String, KV<Integer, String>> {
        private int shardsNumber;
        private Random random;

        AssignRandomKeys(int shardsNumber) {
            this.shardsNumber = shardsNumber;
        }

        @Setup
        public void setup() {
            this.random = new Random();
        }

        @ProcessElement
        public void processElement(DoFn<String, KV<Integer, String>>.ProcessContext c) {
            String event = (String)c.element();
            KV kv = KV.of(this.random.nextInt(this.shardsNumber), event);
            c.output(kv);
        }
    }

    private static class FlattenValues extends DoFn<KV<Integer, Iterable<String>>, String> {
        private final String rootTag;

        public FlattenValues(String rootTag) {
            this.rootTag = rootTag;
        }

        @ProcessElement
        public void processElement(DoFn<KV<Integer, Iterable<String>>, String>.ProcessContext c) throws Exception {
            String result = "";

            String value;
            for(Iterator var3 = ((Iterable)((KV)c.element()).getValue()).iterator(); var3.hasNext(); result = result + value) {
                value = (String)var3.next();
            }

            result = String.format("<%s>", this.rootTag) + result + String.format("</%s>", this.rootTag);
            result = result.replace(",", ";").replace("<bytes>", "")
                    .replace("</bytes>", "");
            String n = decodeText(result, Charset.forName("utf-8"));
            c.output(n);
        }
    }

    private static class ConvertGenericRecordToXmlFn extends DoFn<GenericRecord, String> {

        @ProcessElement
        public void processElement(DoFn<GenericRecord, String>.ProcessContext c) throws Exception {
            GenericRecord record = (GenericRecord)c.element();
            String result = XML.toString(new JSONObject(record.toString()));
            c.output(result);
        }
    }

    private static class GenerateRecordsCountTagFn extends DoFn<Long, String> {

        @ProcessElement
        public void processElement(DoFn<Long, String>.ProcessContext c) throws Exception {
            Long recordsCount = (Long) c.element();
            String result = "<records_count>" + recordsCount.toString() + "</records_count>";
            c.output(result);
        }
    }

    private static class ExtractJmsRecordPayload extends DoFn<JmsRecord, String> {

        @ProcessElement
        public void processElement(
                @Element JmsRecord input,
                OutputReceiver<String> out) {
            out.output(input.getPayload());
        }
    }

    private static class ConvertXmlToJson extends DoFn<String, String> {

        private final String charset;

        private final String xsltString;

        private InputStream xslt;

        private Transformer transformer;

        ConvertXmlToJson(InputStream xslt, String charset) throws IOException {
            this.xsltString = IOUtils.toString(xslt);
            this.charset    = charset;
        }

        @Setup
        public void setup() throws TransformerConfigurationException {
            this.xslt        = new ByteArrayInputStream(xsltString.getBytes());
            this.transformer = new TransformerFactoryImpl().newInstance()
                    .newTransformer(new StreamSource(this.xslt));
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            String inputString = c.element();
            try (InputStream inputStream   = new ByteArrayInputStream(inputString.getBytes(charset));
                 OutputStream outputStream = new ByteArrayOutputStream()) {
                transformer.transform(new StreamSource(inputStream), new StreamResult(outputStream));
                xslt.reset();
                c.output(outputStream.toString());
            }
        }

    }

    private static class ConvertStringToGenericRecord extends DoFn<String, GenericRecord> {

        private final String schemaJson;

        private final String targetField;

        private Schema schema;

        ConvertStringToGenericRecord(Schema schema, String targetField){
            this.schemaJson  = schema.toString();
            this.targetField = targetField;
        }

        @Setup
        public void setup(){
            schema = new Schema.Parser().parse(schemaJson);
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            String input = c.element();
            GenericRecord record = new GenericData.Record(schema);
            record.put(targetField,input);
            c.output(record);
        }
    }

    private static class CustomFileNaming implements FileIO.Write.FileNaming {
        private String prefix;
        private String suffix;

        public CustomFileNaming(String prefix, String suffix) {
            this.prefix = prefix;
            this.suffix = suffix;
        }


        public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
            return String.format("", "%s-%d-of-%d%s", prefix, shardIndex, numShards, suffix);
        }
    }

    public static void main(String[] args) throws Exception {
        JmsExportingOptions options = (JmsExportingOptions) PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(JmsExportingOptions.class);
        String clusterNamespace = options.getClusterNamespace();
        System.out.println("DEBUG. Current namespace: " + clusterNamespace);
        Configuration hdfsConf = new Configuration();
        hdfsConf.set("fs.defaultFS", "hdfs://" + clusterNamespace + ":8020");
        hdfsConf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        options.setHdfsConfiguration(ImmutableList.of(hdfsConf));
        FileSystems.setDefaultPipelineOptions(options);

        for (int i = 0; i < args.length; i++)
            System.out.println("DEBUG: args[" + i + "]: " + args[i]);

        Pipeline pipeline = Pipeline.create(options);

        MQConnectionFactory connectionFactory = new MQConnectionFactory();
        connectionFactory.setTransportType(options.getJmsTransportType());
        connectionFactory.setHostName(options.getJmsBrokerHost());
        connectionFactory.setPort(Integer.valueOf(options.getJmsBrokerPort()));
        connectionFactory.setQueueManager(options.getJmsQueueManager());
        connectionFactory.setChannel(options.getJmsChannel());
        PCollection<GenericRecord> records = null;

        Configuration credentials = new Configuration();
        credentials.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, options.getJmsCredentialProviderPath());
        String jmsPassword = String.valueOf(credentials.getPassword(options.getJmsCredentialAlias()));
        System.out.println("DEBUG: jmsPassword" + jmsPassword);

        Schema schema = null;

        System.out.println("Test1231");

        try {
            InputStream schemaStream = JmsExportingJob.class.getClassLoader()
                    .getResourceAsStream(options.getSourceSchemaPath());

            try {
                Schema.Parser parser = new Schema.Parser();
                schema = parser.parse(schemaStream);
            } catch (Throwable var5) {
                if (schemaStream != null) {
                    try {
                        schemaStream.close();
                    } catch (Throwable var4) {
                        var5.addSuppressed(var4);
                    }
                }

                throw var5;
            }

            if (schemaStream != null) {
                schemaStream.close();
            }
        } catch (IOException var6) {
            JmsExportingJob.log.error(String.format("File %s not found", options.getSourceSchemaPath()));
        }

        System.out.println("Test1232");

        if (options.getDirection().equalsIgnoreCase("export")) {

            System.out.println("Test1233");

            PCollection<String> resultDataset = null;

            if (options.getExportMode().equalsIgnoreCase("export_data") ||
                options.getExportMode().equalsIgnoreCase("export_records_count")) {

                System.out.println("Test1234");

                switch (options.getSourceFormat()) {
                    case "avro":
                        records = pipeline
                                .apply("Ingest data",
                                        AvroIO.readGenericRecords(schema)
                                                .from(options.getSourceDatasetPath()));
                        break;

                    case "parquet":
                        System.out.println("Test3272");
                        PCollection<FileIO.ReadableFile> parquetFiles = pipeline
                                .apply("Match filepattern",
                                        FileIO.match().filepattern(options.getSourceDatasetPath()))
                                .apply("Convert matching results to FileIO.ReadableFile", FileIO.readMatches());

                        records = parquetFiles
                                .apply("Ingest data",
                                        ParquetIO.readFiles(schema));

                        System.out.println("Test3273");

                        break;

                    default:
                        throw new Exception("Unknown format specified for the source dataset");
                }

                System.out.println("Test1235");

                if (options.getExportMode().equalsIgnoreCase("export_data")) {
                    System.out.println("Test1236");
                    resultDataset = records
                            .apply(String.format("Extract fields for dataflow"),
                                    ParDo.of(new GetTargetFieldFn(options.getSourceField(),
                                            options.getTargetField() != "" ? options.getTargetField() :
                                                    options.getSourceField())))
                            .apply("Convert GenericRecord to Xml",
                                    ParDo.of(new ConvertGenericRecordToXmlFn()))
                            .apply(ParDo.of(new AssignRandomKeys(options.getJmsNumberOfBatches())))
                            .apply(GroupIntoBatches.ofSize((long) options.getJmsBatchSize()))
                            .apply(ParDo.of(new FlattenValues(options.getJmsRootTag())));
                    System.out.println("Test1237");
                } else {
                    System.out.println("Test1238");
                    resultDataset = records
                            .apply(Count.globally()).apply(ParDo.of(new GenerateRecordsCountTagFn()));
                    System.out.println("Test1239");
                }
            } else {
                System.out.println("Test12310");
                resultDataset = pipeline.apply(Create.of("<final/>")
                        .withCoder(StringUtf8Coder.of()));
                System.out.println("Test12311");

            }

            System.out.println("Test12312");


            System.out.println("Test12313");

            resultDataset.apply("Write messages", JmsIO.write()
                    .withConnectionFactory(connectionFactory)
                    .withQueue(options.getJmsQueue())
                    .withUsername(options.getJmsUserName())
                    .withPassword(jmsPassword));

            System.out.println("Test12314");

        } else {

            System.out.println("Test12315");

            /*
            JmsPoolConnectionFactory poolConnectionFactory = new JmsPoolConnectionFactory();
            poolConnectionFactory.setConnectionFactory(connectionFactory);
            poolConnectionFactory.setMaxSessionsPerConnection(options.getMaxSessionPerConnection());
            poolConnectionFactory.setMaxConnections(options.getMaxConnections());
             */

            JmsIO.Read<JmsRecord> jmsIORead = JmsIO.<JmsRecord>read()
                .withConnectionFactory(connectionFactory)
                .withQueue(options.getJmsQueue())
                .withUsername(options.getJmsUserName())
                .withPassword(jmsPassword)
                //.withMaxReadTime(Duration.standardMinutes(options.getMaxNumRecords()));
                .withMaxNumRecords(options.getMaxNumRecords());

            PCollection<GenericRecord> recordsToWrite = pipeline
                .apply("Ingest data",
                    jmsIORead)
                .apply("Get message payload",
                    ParDo.of(new ExtractJmsRecordPayload()))
                .apply(
                    "Deserialize data",
                    ParDo.of(new ConvertJsonToGenericRecord(schema, options.getTargetField())))
                    //ParDo.of(new ConvertStringToGenericRecord(schema, options.getTargetField())))
                .setCoder(AvroCoder.of(GenericRecord.class, schema));

            /*
            recordsToWrite
                    .apply(Count.globally()).apply(ParDo.of(new DoFn<Long, String>() {
                        @ProcessElement
                        public void processElement(DoFn<Long, String>.ProcessContext c) throws Exception {
                            Long recordsCount = (Long) c.element();
                            log.info("Сount of PCollection:" + recordsCount);
                        }
                    }));
             */

            System.out.println("Test12316");

            switch (options.getSourceFormat()) {
                case "avro":
                    /*
                    recordsToWrite
                        .apply("Write GenericRecord's",
                            AvroIO.writeGenericRecords(schema)
                                .to(options.getSourceDatasetPath()));
                    */
                    recordsToWrite
                            .apply("write", Window.into(FixedWindows.of(Duration.standardSeconds(30))))
                            .apply("Write Generic Record's",
                                    AvroIO.writeGenericRecords(schema)
                                            .withSchema(schema)
                                            .to("hdfs://ClusterBDA5-ns:8020/user/tech_distcp/card_transactions")
                                            .withNumShards(1)
                                            .withSuffix(".parquet")
                                            .withShardNameTemplate("-SSSSS-of-NNNNN")
                                            .withTempDirectory(FileSystems.matchNewResource("hdfs://ClusterBDA5-ns:8020/user/tech_distcp/card_transactions/tmp", true))
                                            .withWindowedWrites()
                            );
                    break;

                case "parquet":
                    /*
                    recordsToWrite
                            .apply("write", Window.into(FixedWindows.of(Duration.standardMinutes(15))))
                            .apply("Write GenericRecord's",
                                    FileIO.<GenericRecord>write()
                                            .via(ParquetIO.sink(schema))
                                            .withNumShards(1)
                                            .withNoSpilling()
                                            .to("hdfs://ClusterBDA5-ns:8020/user/tech_distcp/card_transactions")
                                            .withTempDirectory("hdfs://ClusterBDA5-ns:8020/user/tech_distcp/card_transactions/tmp")
                                            .withNaming(new CustomFileNaming("card_transaction_ibmmq", ".parquet"))
                            );
                     */
                    PCollection streamedDataWindow = recordsToWrite.apply(Window.<GenericRecord>into(new GlobalWindows())
                            .triggering(Repeatedly.forever(AfterProcessingTime
                                    .pastFirstElementInPane()
                                    .plusDelayOf(Duration.standardMinutes(10)))
                            )
                            //.withAllowedLateness(Duration.standardDays(1))
                            .withAllowedLateness(Duration.ZERO)
                            .discardingFiredPanes()
                    );
                    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                    streamedDataWindow
                            .apply(FileIO.<GenericRecord>write()
                            .via(ParquetIO.sink(schema))
                                    .to(options.getSourceDatasetPath())
                                    .withNumShards(1)
                                    .withPrefix(String.valueOf(timestamp.getTime()))
                                    .withSuffix(".parquet")
                                    //.withNaming(new CustomFileNaming("card_transaction_ibmmq", ".parquet"))
                            //.to("hdfs://ClusterBDA5-ns:8020/user/tech_distcp/card_transactions")
                            //.withTempDirectory("hdfs://ClusterBDA5-ns:8020/user/tech_distcp/card_transactions/tmp")
                            //.withNaming(new CustomFileNaming("card_transaction_ibmmq", ".parquet"))
                    );

                    /*
                    streamedDataWindow.apply(FileIO.<Void, GenericRecord>writeDynamic()
                            .by(element -> null)
                            .via(ParquetIO.sink(schema))
                            .to(options.getSourceDatasetPath())
                            .withNumShards(1));
                     */
                    /*
                    recordsToWrite
                            .apply("write", Window.into(FixedWindows.of(Duration.standardSeconds(50))))
                            .apply("Write GenericRecord's",
                                    FileIO.<GenericRecord>write()
                                            .via(ParquetIO.sink(schema))
                                            .withNumShards(1)
                                            .to("hdfs://ClusterBDA5-ns:8020/user/tech_distcp/card_transactions")
                                            .withNoSpilling()
                            );
                     */
                    break;

                default:
                    throw new Exception("Unknown format specified for the target dataset");
            }
            System.out.println("Test12317");
        }
        pipeline.run().waitUntilFinish();
    }

}