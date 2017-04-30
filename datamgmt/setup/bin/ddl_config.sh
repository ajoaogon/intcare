#!/usr/bin/env bash
echo "** Creating Hive tables ..."

runuser -l hdfs -c "hdfs dfs -mkdir /user/root"
runuser -l hdfs -c "hdfs dfs -chmod -R 777 /tmp"
hive -f setup/ddl/hl7_events.ddl

runuser -l hdfs -c "hdfs dfs -chown -R storm /apps/hive/warehouse/hl7_events_text_partition"
runuser -l hdfs -c "hdfs dfs -chown -R storm /apps/hive/warehouse/hl7_events_orc_partition_single"


echo "** Creating HBase tables ..."

echo "** Creating hl7_events table ..."
echo "create 'hl7_events', {NAME=> 'events', VERSIONS=>3}" | hbase shell

echo "** Creating hl7_critical_events table ..."
echo "create 'hl7_critical_events', {NAME=> 'critical', VERSIONS=>3}" | hbase shell

echo "** Creating hl7_critical_events_count table ..."
echo "create 'hl7_critical_events_count', {NAME=> 'count', VERSIONS=>3}" | hbase shell
