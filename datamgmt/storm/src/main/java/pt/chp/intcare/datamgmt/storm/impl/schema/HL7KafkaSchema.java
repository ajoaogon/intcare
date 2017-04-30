package pt.chp.intcare.datamgmt.storm.impl.schema;

import org.apache.log4j.Logger;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import pt.chp.intcare.datamgmt.storm.impl.constants.HL7Constants;
import pt.chp.intcare.datamgmt.storm.utils.DataUtils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

public class HL7KafkaSchema implements Scheme {

    private static final long serialVersionUID = -4832451960234934643L;
    private static final Logger LOGGER = Logger.getLogger(HL7KafkaSchema.class);


    @Override
    public List<Object> deserialize(ByteBuffer byteBuffer) {

        try {
            byte[] bytes = Utils.toByteArray(byteBuffer);
            String hl7Event = new String(bytes, "UTF-8");
            String[] dataElem = hl7Event.split("\\|");

            String msgId = dataElem[0];
            String source = dataElem[1];
            String msgDate = dataElem[2];
            String valueDate = dataElem[3];
            long incident = Long.valueOf(dataElem[4]);
            String module = dataElem[5];
            String category = dataElem[6];
            String machine = dataElem[7];
            double value = Double.valueOf(dataElem[8]);
            String unit = dataElem[9];
            String valid = DataUtils.parseEmptyToNull(dataElem[11]);
            String critical = DataUtils.parseEmptyToNull(dataElem[12]);
            long auxSequence = Long.valueOf(dataElem[13]);

            String eventKey = constructKey(msgId, incident, machine, auxSequence);

            LOGGER.info("Creating a HL7 Scheme with msgId[" + msgId + "], incident[" + incident + "], machine["
                    + machine + "] and auxSequence[" + auxSequence + "]");

            return new Values(eventKey, msgId, source, msgDate, valueDate, incident, module, category, machine, value, unit,
                    valid, critical, auxSequence);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Fields getOutputFields() {
        return HL7Constants.getFields();
    }

    private String constructKey(String msgId, long incident, String machine, long auxSequence) {
        StringBuilder sb = new StringBuilder();
        sb.append(msgId.replaceAll(" ", "")).append("_")
                .append(incident).append("_")
                .append(machine.replaceAll(" ", "")).append("_")
                .append(auxSequence);

        return sb.toString();
    }
}
