package utils;

import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import rdf.RDFReading;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by tsotzo on 14/5/2017.
 */
public class ReadPropertiesFile {

    public static String readRDFDataInputPath() throws IOException {
    Properties prop = new Properties();
    InputStream input = null;
    String inputPath = "";
        try {
            input = RDFReading.class.getClassLoader().getResourceAsStream("config.properties");
            prop.load(input);
            //Getting the setting from the property file
            inputPath = prop.getProperty("RDFDataInputPath");
        } catch (IOException e) {
            e.printStackTrace();
        }

        return inputPath;
    }


    public static String readHDFSDataInputPath() throws IOException {
        Properties prop = new Properties();
        InputStream input = null;
        String inputPath = "";
        try {
            input = RDFReading.class.getClassLoader().getResourceAsStream("config.properties");
            prop.load(input);
            //Getting the setting from the property file
            inputPath = prop.getProperty("HDFSDataPath").toString();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return inputPath;
    }


}
