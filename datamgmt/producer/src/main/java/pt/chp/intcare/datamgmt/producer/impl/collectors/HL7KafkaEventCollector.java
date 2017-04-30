package pt.chp.intcare.datamgmt.producer.impl.collectors;

import pt.chp.intcare.datamgmt.producer.impl.producers.HL7KafkaProducer;
import pt.chp.intcare.datamgmt.producer.utils.FileUtils;

import java.io.File;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class HL7KafkaEventCollector extends BaseEventCollector {


    public HL7KafkaEventCollector(String configFileLocation) throws Exception {
        super(configFileLocation);
    }

    public static void main(String[] args) throws Exception {
        long now = new Date().getTime();
        System.out.println("Starting Collector ...");

        String configFileLocation = args[0];

        HL7KafkaEventCollector collector = new HL7KafkaEventCollector(configFileLocation);
        collector.send(args[1]);
        System.out.println("Collector duration [" + (new Date().getTime() - now) + "] milliseconds");
    }

    private void send(String filesPath) {
        int timeInMinutes = Integer.valueOf(collectorConfig.getProperty("hdfs.hl7.file.rotation.time.minutes"));

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

        try {
            LocalDateTime next;

            File[] files = FileUtils.listFiles(filesPath);
            for (File file : files) {
                if (file.isFile()) {
                    next = LocalDateTime.now().plusMinutes(timeInMinutes);

                    new HL7KafkaProducer(collectorConfig, file).process();

                    Thread.sleep(ChronoUnit.MILLIS.between(LocalDateTime.now(), next));
                }
            }
        } catch (Exception e) {
            System.out.println("Error processing files ...");
            e.printStackTrace();
        } finally {
            executorService.shutdown();
        }
    }
}
