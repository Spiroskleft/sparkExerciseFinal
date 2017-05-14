package utils;

import rdf.RDFReading;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by tsotzo on 14/5/2017.
 */
public class ReadPropertiesFile {
    public static  String inputPath ;

    public static void readFile() throws IOException {
    Properties prop = new Properties();
    InputStream input = null;
        try {
            input = RDFReading.class.getClassLoader().getResourceAsStream("config.properties");
            prop.load(input);
            //Getting the setting from the property file
            inputPath = prop.getProperty("RDFDataInputPath");
            System.out.println("---------------InputPath------------->"+inputPath);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
