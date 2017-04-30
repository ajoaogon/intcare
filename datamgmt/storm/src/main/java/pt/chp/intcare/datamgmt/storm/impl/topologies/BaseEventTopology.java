package pt.chp.intcare.datamgmt.storm.impl.topologies;

import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.util.Properties;

public abstract class BaseEventTopology {

    private static final Logger LOGGER = Logger.getLogger(BaseEventTopology.class);

    protected Properties topologyConfig;


    public BaseEventTopology(String configFileLocation) throws Exception {
        topologyConfig = new Properties();

        try {
            topologyConfig.load(new FileInputStream(configFileLocation));
        } catch (Exception e) {
            LOGGER.error("Encountered error while reading configuration properties: " + e.getMessage());
            throw e;
        }
    }
}
