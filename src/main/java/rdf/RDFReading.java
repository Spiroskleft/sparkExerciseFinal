package rdf;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import rdf.basicGraphPatterns.FindBasicGraphPatterns;
import rdf.joins.RdfJoins;
import utils.RdfTrasformation;

import java.io.IOException;

import static utils.ReadPropertiesFile.readConfigProperty;
import static utils.ReadPropertiesFile.readRunProperty;

/**
 * Created by tsotzolas on 02/05/2017.
 * Ειναι η main κλαση για να μπορεί να τρέχει στο Spark
 * Όλες οι ρυθμήσεις γίνονται σε συνεργασίας με τα αρχεία config.properties και run.properties
 * Για να το τρέξουμε σε ένα Spark Cluster
 * spark-submit --class rdf.RDFReading  --master spark://<master's_ip>:<master's_port>  sparkExerciseFinal-1.0-SNAPSHOT.jar
 * spark-submit --class rdf.RDFReading  --master spark://10.0.0.5:7077  sparkExerciseFinal-1.0-SNAPSHOT.jar
 */
public class RDFReading {
    private static SparkSession sparkSession;

    public static void main(String[] args) throws IOException, AnalysisException {


        //Create the Spark session
        sparkSession = SparkSession.builder().appName("RDFreader").getOrCreate();

        //Transformations
        if ("true".equals(readRunProperty("parceTxtToVP"))) {
            RdfTrasformation.parceTxtToVP();
        }

        if ("true".equals(readRunProperty("tranformCSVtoParquet"))) {
            RdfTrasformation.tranformCSVtoParquet(readConfigProperty("CSVFileName"), readConfigProperty("inputCSVPath"), readConfigProperty("outputParquetPath"), sparkSession);
        }


        //Basic Graph Patterns
        if ("true".equals(readRunProperty("findObject"))) {
            //s1 p1 ?o
            FindBasicGraphPatterns.findObject("<http://data.bgs.ac.uk/id/Geochronology/DivisionList/CAA>", "0", sparkSession, readConfigProperty("BGPInputFileType"));
        }
        if ("true".equals(readRunProperty("findSubject"))) {
            //?s p1 o1
            FindBasicGraphPatterns.findSubject("<http://www.w3.org/2004/02/skos/core#OrderedCollection>", "0", sparkSession, readConfigProperty("BGPInputFileType"));
        }

        if ("true".equals(readRunProperty("findSubjectObject"))) {

            //?s p1 ?o
            FindBasicGraphPatterns.findSubjectObject("0", sparkSession, readConfigProperty("BGPInputFileType"));
        }


        //Joins
        if ("true".equals(readRunProperty("findSubjectSubjectJoin"))) {

            //?s p1 o1
            //?s p2 o2
            //Πρέπει να βγάλει a
            RdfJoins.findSubjectSubjectJoin("follows", "likes", "b", "i1", sparkSession, readConfigProperty("joinInputFileType"));
        }

        if ("true".equals(readRunProperty("findObjectObjectJoin"))) {
            //s1 p1 ?o
            //s2 p1 ?o
            //Πρέπει να βγάλει a
            RdfJoins.findObjectObjectJoin("follows", "follows", "a", "c", sparkSession, readConfigProperty("joinInputFileType"));
        }

        if ("true".equals(readRunProperty("findObjectSubjectJoin"))) {
            //s1 p1 ?o
            //?s p2 o2
            //Πρέπει να βγάλει c
            RdfJoins.findObjectSubjectJoin("follows", "likes", "b", "i2", sparkSession, readConfigProperty("joinInputFileType"));
        }


        if ("true".equals(readRunProperty("findObjectSubjectJoin"))) {
            //?s p2 o2
            //s1 p1 ?o
            //Πρέπει να βγάλει c
        RdfJoins.findSubjectObjectJoin("likes","follows","i2","b",sparkSession,readConfigProperty("joinInputFileType"));
        }
    }

}
