create table hl7_events_text_partition(
    msgId       string,
    source      string,
    msgDate     string,
    valueDate   string,
    incident    bigint,
    module      string,
    category    string,
    machine     string,
    value       double,
    unit        string,
    valid       string,
    critical    string,
    auxSequence bigint,
    eventkey    string
)
partitioned by (eventDate string)
row format delimited fields terminated by ','
stored as textfile;

create table hl7_events_orc_partition_single(
    msgId       string,
    source      string,
    msgDate     string,
    valueDate   string,
    incident    bigint,
    module      string,
    category    string,
    machine     string,
    value       double,
    unit        string,
    valid       string,
    critical    string,
    auxSequence bigint,
    eventkey    string
)
partitioned by (eventDate string)
row format serde 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
stored as orc;
