## Setup Instructions

### Storm commands
Storm is installed on `ingest.datamgmt.intcare`
1. Install Topology: `storm jar /root/datamgmt/storm/target/datamgmt-storm-0.1.jar pt.chp.intcare.datamgmt.storm.impl.topologies.HL7EventTopology /root/datamgmt/config.properties`


### HBase commands
HBase is installed on `master.datamgmt.intcare`
1. Access HBase: `hbase shell`
2. Create tables:
- `create 'hl7_events', 'events'`
- `create 'hl7_critical_events', 'critical'`
- `create 'hl7_critical_events_count', 'count'`
3. List all tables: `list`
4. Truncate tables:
- `truncate 'hl7_events'`
- `truncate 'hl7_critical_events'`
- `truncate 'hl7_critical_events_count'`


### Kafka commands
Kafka is installed on `ingest.datamgmt.intcare`
1. Create topic: `/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper master.datamgmt.intcare:2181 --replication-factor 1 --partitions 2 --topic hl7_events_topic`
2. List topics: `/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper master.datamgmt.intcare:2181`
3. View published messages: `/usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --zookeeper master.datamgmt.intcare:2181 --topic hl7_events_topic --from-beginning`
4. Delete topic: `/usr/hdp/current/kafka-broker/bin//kafka-topics.sh --zookeeper master.datamgmt.intcare:2181 --delete --topic hl7_events_topic`