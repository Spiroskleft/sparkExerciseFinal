import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by tsotzolas on 02/05/2017.
 * ---------------------------------------------------------------------
 * Είναι το τρίτο μέρος απο την εργασία DataFrames and DataSets την οποία μας έδωσε ο καθηγητής.
 * ---------------------------------------------------------------------
 * <p>
 * Για να το τρέξεις
 * spark-submit --class spark.ExerciseRDF  sparkExerciseRDF-1.0-SNAPSHOT.jar
 */

public class ExerciseRDF {
    private static String inputFile;
    private static  String inputPath ;
    private static String outputDirectory;
    private static SparkSession sparkSession;

    public static void main(String[] args) throws IOException {

        Properties prop = new Properties();
        InputStream input = null;
        input = JavaParseTxt.class.getClassLoader().getResourceAsStream("config.properties");

        // load a properties file
        prop.load(input);
        //Getting the setting from the property file
        inputPath = prop.getProperty("RDFDataInputPath");
        //Create the Spark session
        sparkSession = SparkSession.builder().master("local").appName("RDFreader").getOrCreate();


        //s1 p1 ?o
        findObject("<http://data.bgs.ac.uk/id/Geochronology/DivisionList/CAA>","0",sparkSession);

        //?s p1 o1
        findSubject("<http://www.w3.org/2004/02/skos/core#OrderedCollection>","0",sparkSession);
    }


    /**
     * Working with triples (s,p,o)
     * when triples in verticalPartitioning (VP)
     * s1 p1 ?o
     * @param subject
     * @param predicate
     */
    public static void findObject(String subject, String predicate,SparkSession sparkSession){
        //The predicate will tell us the file that we must take
        //Φορτώνουμε το αρχειο σε ένα Dataset
        Dataset<Row> df = sparkSession.read().csv(inputPath+predicate+".csv");

        df.createOrReplaceTempView("tableName");
        //Κάνουμε προβολή των δεδομένων
        sparkSession.sql("SELECT _c1 as object " +
                "FROM tableName " +
                "where _c0='"+subject+"'").show();
    }


    /**
     * Working with triples (s,p,o)
     * when triples in verticalPartitioning (VP)
     * ?s p1 o1
     * @param object
     * @param predicate
     */
    public static void findSubject(String object, String predicate,SparkSession sparkSession){
        //The predicate will tell us the file that we must take
        //Φορτώνουμε το αρχειο σε ένα Dataset
        Dataset<Row> df = sparkSession.read().csv(inputPath+predicate+".csv");

        df.createOrReplaceTempView("tableName");
        //Κάνουμε προβολή των δεδομένων
        sparkSession.sql("SELECT _c0 as subject " +
                "FROM tableName " +
                "where _c1='"+object+"'").show();
    }

}
