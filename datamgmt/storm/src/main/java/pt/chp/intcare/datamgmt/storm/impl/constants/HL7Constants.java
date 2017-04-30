package pt.chp.intcare.datamgmt.storm.impl.constants;

import org.apache.storm.tuple.Fields;

public class HL7Constants {

    public static final String EVENTS_TABLE_NAME = "hl7_events";
    public static final String EVENTS_TABLE_COLUMN_FAMILY_NAME = "events";

    public static final String CRITICAL_EVENTS_TABLE_NAME = "hl7_critical_events";
    public static final String CRITICAL_EVENTS_TABLE_COLUMN_FAMILY_NAME = "critical";

    public static final String CRITICAL_EVENTS_COUNT_TABLE_NAME = "hl7_critical_events_count";
    public static final String CRITICAL_EVENTS_COUNT_TABLE_COLUMN_FAMILY_NAME = "count";

    public static final String INCIDENT_FIELD_NAME = "incident";
    public static final String EVENT_KEY_FIELD_NAME = "eventKey";


    public static Fields getFields() {
        return new Fields(EVENT_KEY_FIELD_NAME, "msgId", "source", "msgDate", "valueDate", INCIDENT_FIELD_NAME, "module", "category", "machine",
                "value", "unit", "valid", "critical", "auxSequence");
    }

    public static Fields getIncidentField() {
        return new Fields(INCIDENT_FIELD_NAME);
    }

    public static Fields getCriticalEventsCountFields() {
        return new Fields(INCIDENT_FIELD_NAME, "criticalEventsTotal");
    }

    public static Fields getCriticalEventsTotalField() {
        return new Fields("criticalEventsTotal");
    }
}
