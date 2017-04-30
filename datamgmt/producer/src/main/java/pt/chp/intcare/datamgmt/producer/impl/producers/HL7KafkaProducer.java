package pt.chp.intcare.datamgmt.producer.impl.producers;

import org.apache.kafka.clients.producer.*;
import pt.chp.intcare.datamgmt.producer.utils.FileUtils;

import java.io.File;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class HL7KafkaProducer {

    private Properties collectorConfig;
    private File file;


    public HL7KafkaProducer(Properties collectorConfig, File file) {
        this.collectorConfig = collectorConfig;
        this.file = file;
    }


    public void process() {
        Producer<String, String> producer = null;

        try {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, collectorConfig.getProperty("kafka.bootstrap.servers"));
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

            producer = new KafkaProducer<>(props);

            String topic = collectorConfig.getProperty("kafka.hl7.topic");

            List<String> lines = FileUtils.readFile(file);

            Date start = new Date();
            System.out.println("Start send of events of file [" + file.getPath() + "] at [" + start + "]");
            for (String line : lines) {
                producer.send(new ProducerRecord<>(topic, line), new HL7KafkaProducer.ProducerCallback());
            }

            Date end = new Date();
            System.out.println("End send of events at [" + end + "]. Sent/ processed [" + lines.size() + "] events in [" + (end.getTime() - start.getTime()) + "] milliseconds");
        } catch (Exception e) {
            System.out.println("Error processing file[" + file.getPath() + "]");
            e.printStackTrace();
        } finally {
            if (producer != null)
                producer.close();
        }
    }


    private class ProducerCallback implements Callback {

        @Override
        public void onCompletion(RecordMetadata metadata, Exception e) {
            if (e != null) {
                System.out.println("Error producing to topic " + metadata.topic());
                e.printStackTrace();
            }
        }
    }
}
