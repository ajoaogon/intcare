package pt.chp.intcare.datamgmt.storm.impl.topologies;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import pt.chp.intcare.datamgmt.storm.impl.bolts.HL7RouteBolt;
import pt.chp.intcare.datamgmt.storm.impl.constants.HL7Constants;
import pt.chp.intcare.datamgmt.storm.impl.schema.HL7KafkaSchema;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HL7EventTopology extends BaseEventTopology {

    private static final Logger LOGGER = Logger.getLogger(HL7EventTopology.class);

    // Spout and Bolt names
    private static final String KAFKA_SPOUT = "schema-spout";
    private static final String HDFS_BOLT = "hdfs-bolt";
    private static final String ROUTE_BOLT = "route-bolt";
    private static final String COUNT_BOLT = "count-bolt";
    private static final String HBASE_EVENTS_BOLT = "hbase-events-bolt";
    private static final String HBASE_CRITICAL_EVENTS_BOLT = "hbase-critical-events-bolt";
    private static final String HBASE_CRITICAL_EVENTS_COUNT_BOLT = "hbase-critical-events-count-bolt";


    public HL7EventTopology(String configFileLocation) throws Exception {
        super(configFileLocation);
    }


    public static void main(String[] args) throws Exception {
        String configFileLocation = args[0];

        HL7EventTopology topology = new HL7EventTopology(configFileLocation);
        topology.buildAndSubmit();
    }


    private void buildAndSubmit() {
        TopologyBuilder builder = new TopologyBuilder();

        configureKafkaSpout(builder);

        configureRouteBolt(builder);

        configureCountBolt(builder);

        configureHDFSBolt(builder);

        configureHBaseBolt(builder);


        Config config = new Config();
        config.setDebug(true);

        Map<String, Object> hbaseConf = new HashMap<>();
        config.put("hbase.conf", hbaseConf);

        Integer topologyWorkers = Integer.valueOf(topologyConfig.getProperty("storm.hl7.topology.workers"));
        config.put(Config.TOPOLOGY_WORKERS, topologyWorkers);

        List<String> nimbusSeeds = Arrays.asList(topologyConfig.getProperty("nimbus.seeds").split(","));
        config.put(Config.NIMBUS_SEEDS, nimbusSeeds);


        try {
            StormSubmitter.submitTopology("hl7-event-topology", config, builder.createTopology());
        } catch (Exception e) {
            LOGGER.error("Error submiting Topology", e);
        }
    }

    private void configureKafkaSpout(TopologyBuilder builder) {
        BrokerHosts hosts = new ZkHosts(topologyConfig.getProperty("kafka.zookeeper.host.port"));
        String topic = topologyConfig.getProperty("kafka.hl7.topic");
        String zkRoot = topologyConfig.getProperty("kafka.hl7.zkRoot");
        String id = topologyConfig.getProperty("kafka.hl7.consumer.group.id");

        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, id);
        spoutConfig.scheme = new SchemeAsMultiScheme(new HL7KafkaSchema());

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        int spoutThreadCount = Integer.valueOf(topologyConfig.getProperty("storm.hl7.spout.kafka.thread.count"));

        builder.setSpout(KAFKA_SPOUT, kafkaSpout, spoutThreadCount);
    }

    private void configureRouteBolt(TopologyBuilder builder) {
        boolean persistAllEvents = Boolean.valueOf(topologyConfig.getProperty("hbase.hl7.persist.all.events"));
        int hbaseBoltCount = Integer.valueOf(topologyConfig.getProperty("hbase.hl7.bolt.thread.count"));

        builder.setBolt(ROUTE_BOLT, new HL7RouteBolt(persistAllEvents), hbaseBoltCount)
                .shuffleGrouping(KAFKA_SPOUT);
    }

    private void configureCountBolt(TopologyBuilder builder) {
        /*int hbaseBoltCount = Integer.valueOf(topologyConfig.getProperty("hbase.hl7.bolt.thread.count"));

        builder.setBolt(COUNT_BOLT, new HL7CountBolt(), hbaseBoltCount)
                .shuffleGrouping(ROUTE_BOLT, "CRITICAL");*/
    }

    private void configureHDFSBolt(TopologyBuilder builder) {
        String rootPath = topologyConfig.getProperty("hdfs.hl7.path");
        String prefix = topologyConfig.getProperty("hdfs.hl7.file.prefix");
        String fsUrl = topologyConfig.getProperty("hdfs.url");
        Float rotationTimeInMinutes = Float.valueOf(topologyConfig.getProperty("hdfs.hl7.file.rotation.time.minutes"));


        RecordFormat recordFormat = new DelimitedRecordFormat().withFieldDelimiter("|");

        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        TimedRotationPolicy rotationPolicy = new TimedRotationPolicy(
                rotationTimeInMinutes, TimedRotationPolicy.TimeUnit.MINUTES);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath(rootPath + "/staging")
                .withPrefix(prefix)
                .withExtension(".csv");

        HdfsBolt hdfsBolt = new HdfsBolt()
                .withFsUrl(fsUrl)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(recordFormat)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        int hdfsBoltCount = Integer.valueOf(topologyConfig.getProperty("hdfs.hl7.bolt.thread.count"));

        builder.setBolt(HDFS_BOLT, hdfsBolt, hdfsBoltCount)
                .fieldsGrouping(ROUTE_BOLT, HL7Constants.getFields());
    }

    private void configureHBaseBolt(TopologyBuilder builder) {
        int hbaseBoltCount = Integer.valueOf(topologyConfig.getProperty("hbase.hl7.bolt.thread.count"));

        HBaseBolt hbaseEventsBolt = new HBaseBolt(HL7Constants.EVENTS_TABLE_NAME, getEventsHBaseMapper())
                .withConfigKey("hbase.conf");
        builder.setBolt(HBASE_EVENTS_BOLT, hbaseEventsBolt, hbaseBoltCount)
                .fieldsGrouping(ROUTE_BOLT, HL7Constants.getFields());

        HBaseBolt hbaseCriticalEventsBolt = new HBaseBolt(HL7Constants.CRITICAL_EVENTS_TABLE_NAME, getCriticalEventsHBaseMapper())
                .withConfigKey("hbase.conf");
        builder.setBolt(HBASE_CRITICAL_EVENTS_BOLT, hbaseCriticalEventsBolt, hbaseBoltCount)
                .fieldsGrouping(ROUTE_BOLT, "CRITICAL", HL7Constants.getFields());

        /*HBaseBolt hbaseCriticalEventsCountBolt = new HBaseBolt(HL7Constants.CRITICAL_EVENTS_COUNT_TABLE_NAME, getCriticalEventsCountHBaseMapper())
                .withConfigKey("hbase.conf");
        builder.setBolt(HBASE_CRITICAL_EVENTS_COUNT_BOLT, hbaseCriticalEventsCountBolt, hbaseBoltCount)
                .fieldsGrouping(COUNT_BOLT, HL7Constants.getIncidentField());*/
    }

    private SimpleHBaseMapper getEventsHBaseMapper() {
        return new SimpleHBaseMapper()
                .withRowKeyField(HL7Constants.EVENT_KEY_FIELD_NAME)
                .withColumnFields(HL7Constants.getFields())
                .withColumnFamily(HL7Constants.EVENTS_TABLE_COLUMN_FAMILY_NAME);
    }

    private SimpleHBaseMapper getCriticalEventsHBaseMapper() {
        return new SimpleHBaseMapper()
                .withRowKeyField(HL7Constants.EVENT_KEY_FIELD_NAME)
                .withColumnFields(HL7Constants.getFields())
                .withColumnFamily(HL7Constants.CRITICAL_EVENTS_TABLE_COLUMN_FAMILY_NAME);
    }

    private SimpleHBaseMapper getCriticalEventsCountHBaseMapper() {
        return new SimpleHBaseMapper()
                .withRowKeyField(HL7Constants.INCIDENT_FIELD_NAME)
                .withColumnFields(HL7Constants.getIncidentField())
                .withCounterFields(HL7Constants.getCriticalEventsTotalField())
                .withColumnFamily(HL7Constants.CRITICAL_EVENTS_COUNT_TABLE_COLUMN_FAMILY_NAME);
    }
}
