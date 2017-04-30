#!/usr/bin/env bash
echo "-------------------------"
echo "Setting up 'master' requirements ..."
echo "-------------------------"

echo "*** Making HDFS directories ..."
runuser -l hdfs -c "hadoop fs -mkdir -p /hl7-events/staging"
runuser -l hdfs -c "hadoop fs -chown -R storm /hl7-events"

runuser -l hdfs -c "hadoop fs -mkdir /user/root"
runuser -l hdfs -c "hadoop fs -chown root /user/root"

chmod -R 777 /tmp/hive


echo "*** Creating Hive and HBase tables to store events ..."
setup/bin/ddl_config.sh
