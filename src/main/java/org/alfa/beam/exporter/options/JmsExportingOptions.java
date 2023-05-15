package org.alfa.beam.exporter.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.PipelineOptions;

public interface JmsExportingOptions extends PipelineOptions, HadoopFileSystemOptions {

    @Default.String("export")
    String getDirection();

    void setDirection(String value);

    @Default.String("export_data")
    String getExportMode();

    void setExportMode(String value);

    @Default.String("parquet")
    String getSourceFormat();

    void setSourceFormat(String value);

    @Default.String("schemas/d_nrf/transactionProdScoreSummary.avsc")
    String getSourceSchemaPath();

    void setSourceSchemaPath(String value);

    @Default.String("")
    String getSourceDatasetPath();

    void setSourceDatasetPath(String value);

    String getSourceField();

    void setSourceField(String value);

    String getTargetField();

    void setTargetField(String value);

    @Default.Integer(1)
    Integer getJmsTransportType();

    void setJmsTransportType(Integer value);

    String getJmsBrokerHost();

    void setJmsBrokerHost(String value);

    @Default.Integer(1414)
    Integer getJmsBrokerPort();

    void setJmsBrokerPort(Integer value);

    String getJmsChannel();

    void setJmsChannel(String value);

    String getJmsQueue();

    void setJmsQueue(String value);

    String getJmsQueueManager();

    void setJmsQueueManager(String value);

    String getJmsUserName();

    void setJmsUserName(String value);

    String getJmsCredentialAlias();

    void setJmsCredentialAlias(String value);

    String getJmsCredentialProviderPath();

    void setJmsCredentialProviderPath(String value);

    Integer getJmsNumberOfBatches();

    void setJmsNumberOfBatches(Integer value);

    Integer getJmsBatchSize();

    void setJmsBatchSize(Integer value);

    String getJmsRootTag();

    void setJmsRootTag(String value);

    @Default.String("ClusterBDA5-ns")
    String getClusterNamespace();

    void setClusterNamespace(String value);

    @Default.Integer(1000)
    Integer getMaxNumRecords();

    void setMaxNumRecords(Integer value);


    /*
    @Default.Integer(5)
    Integer getMaxSessionPerConnection();

    void setMaxSessionPerConnection(Integer value);

    @Default.Integer(1)
    Integer getMaxConnections();

    void setMaxConnections(Integer value);

     */
}
