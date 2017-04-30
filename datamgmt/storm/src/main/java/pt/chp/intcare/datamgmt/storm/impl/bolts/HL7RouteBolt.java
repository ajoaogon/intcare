package pt.chp.intcare.datamgmt.storm.impl.bolts;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import pt.chp.intcare.datamgmt.storm.impl.constants.HL7Constants;

import java.util.Map;

public class HL7RouteBolt extends BaseRichBolt {

    private static final Logger LOGGER = Logger.getLogger(HL7RouteBolt.class);

    private boolean persistAllEvents;
    private OutputCollector collector;


    public HL7RouteBolt(boolean persistAllEvents) {
        this.persistAllEvents = persistAllEvents;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String msgId = input.getStringByField("msgId");
        String source = input.getStringByField("source");
        String msgDate = input.getStringByField("msgDate");
        String valueDate = input.getStringByField("valueDate");
        long incident = input.getLongByField("incident");
        String module = input.getStringByField("module");
        String category = input.getStringByField("category");
        String machine = input.getStringByField("machine");
        double value = input.getDoubleByField("value");
        String unit = input.getStringByField("unit");
        String valid = input.getStringByField("valid");
        String critical = input.getStringByField("critical");
        long auxSequence = input.getLongByField("auxSequence");
        String eventKey = input.getStringByField("eventKey");

        if ("1".equals(critical) || "2".equals(critical)) {
            LOGGER.debug("[HL7] Processing CRITICAL event with eventKey[" + eventKey + "]");
            collector.emit("CRITICAL", input,
                    new Values(eventKey, msgId, source, msgDate, valueDate, incident, module, category, machine,
                            value, unit, valid, critical, auxSequence));
        }

        if (persistAllEvents) {
            collector.emit(input,
                    new Values(eventKey, msgId, source, msgDate, valueDate, incident, module, category, machine,
                            value, unit, valid, critical, auxSequence));
        }

        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(HL7Constants.getFields());
        declarer.declareStream("CRITICAL", HL7Constants.getFields());
    }
}
