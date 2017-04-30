#!/usr/bin/env bash
echo "*** Creating hl7_events topic in Kafka ..."
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper master.datamgmt.intcare:2181 --replication-factor 1 --partitions 2 --topic hl7_events_topic

echo "*** Deploying the storm topology ..."
storm jar /root/datamgmt/storm/target/datamgmt-storm-0.1.jar pt.chp.intcare.datamgmt.storm.impl.topologies.HL7EventTopology /root/datamgmt/config.properties