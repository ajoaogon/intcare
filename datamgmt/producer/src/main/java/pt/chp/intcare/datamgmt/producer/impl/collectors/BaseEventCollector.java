package pt.chp.intcare.datamgmt.producer.impl.collectors;

import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.util.Properties;

public abstract class BaseEventCollector {

    private static final Logger LOGGER = Logger.getLogger(BaseEventCollector.class);

    protected Properties collectorConfig;


    public BaseEventCollector(String configFileLocation) throws Exception {
        collectorConfig = new Properties();

        try {
            collectorConfig.load(new FileInputStream(configFileLocation));
        } catch (Exception e) {
            LOGGER.error("Encountered error while reading configuration properties: " + e.getMessage());
            throw e;
        }
    }
}
