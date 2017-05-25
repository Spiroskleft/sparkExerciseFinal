package utils;

import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import rdf.RDFReading;
import rdf.joins.RdfJoins;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by tsotzo on 14/5/2017.
 */
public class ReadPropertiesFile {
private static InputStream input = ReadPropertiesFile.class.getClassLoader().getResourceAsStream("config.properties");
    public static String readRDFDataInputPath() throws IOException {
        Properties prop = new Properties();
        String inputPath = "";
        try {
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

    public static String ReadFilePath() throws IOException {
        Properties prop = new Properties();
        String outputQueries;
        // load a properties file
        prop.load(input);
        outputQueries = prop.getProperty("outputQueries");
        return outputQueries;
    }


}
