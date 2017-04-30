package pt.chp.intcare.datamgmt.storm.impl.bolts;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import pt.chp.intcare.datamgmt.storm.impl.constants.HL7Constants;

import java.util.Map;

public class HL7CountBolt extends BaseRichBolt {

    private static final Logger LOGGER = Logger.getLogger(HL7CountBolt.class);

    private OutputCollector collector;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Long incident = input.getLongByField(HL7Constants.INCIDENT_FIELD_NAME);
        LOGGER.debug("[HL7] Processing CRITICAL COUNT event with incident[" + incident + "], eventKey[" + input.getStringByField("eventKey") + "]" +
                ", tuple stream[" + input.getSourceStreamId() + "] and tuple source component[" + input.getSourceComponent() + "]");

        collector.emit(Utils.tuple(incident, 1));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(HL7Constants.getCriticalEventsCountFields());
    }
}
