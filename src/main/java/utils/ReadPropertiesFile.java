package utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by tsotzo on 14/5/2017.
 */
public class ReadPropertiesFile {

private static InputStream inputConfig = ReadPropertiesFile.class.getClassLoader().getResourceAsStream("config.properties");
private static InputStream inputRun = ReadPropertiesFile.class.getClassLoader().getResourceAsStream("run.properties");

    /**
     * Μέθοδος όπου διαβάσει απο το config.properties και επιστρέφει την τιμή σύμφωνα με το param
     * @param propertyName είναι το όνομα του P
     * @return  την τιμή του property απο το config.properties αρχείο
     * @throws IOException
     */
    public static String readConfigProperty(String propertyName) throws IOException {
        Properties prop = new Properties();
        String returnedProperty = "";
        try {
            prop.load(inputConfig);
            //Getting the setting from the property file
            returnedProperty = prop.getProperty(propertyName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return returnedProperty;
    }

    /**
     * Μέθοδος όπου διαβάσει απο το config.properties και επιστρέφει την τιμή σύμφωνα με το param
     * @param propertyName είναι το όνομα του P
     * @return  την τιμή του property απο το config.properties αρχείο
     * @throws IOException
     */
    public static String readRunProperty(String propertyName) throws IOException {
        Properties prop = new Properties();
        String returnedProperty = "";
        try {
            prop.load(inputRun);
            //Getting the setting from the property file
            returnedProperty = prop.getProperty(propertyName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return returnedProperty;
    }



}
