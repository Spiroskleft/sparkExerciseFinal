package rdf;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

import rdf.basicGraphPatterns.*;
import rdf.joins.RdfJoins;

/**
 * Created by tsotzolas on 02/05/2017.
 * ---------------------------------------------------------------------
 * Είναι το τρίτο μέρος απο την εργασία DataFrames and DataSets την οποία μας έδωσε ο καθηγητής.
 * ---------------------------------------------------------------------
 * <p>
 * Για να το τρέξεις
 * spark-submit --class rdf.RDFReading  sparkExerciseFinal-1.0-SNAPSHOT.jar
 */

public class RDFReading {
    private static String inputFile;
    private static  String inputPath ;
    private static String outputDirectory;
    private static SparkSession sparkSession;

    public static void main(String[] args) throws IOException, AnalysisException {
        //TODO να αλλάζουμε να διαβάζει απο το HDFS


        //Create the Spark session
        sparkSession = SparkSession.builder().master("local").appName("RDFreader").getOrCreate();


        //s1 p1 ?o
        FindBasicGraphPatterns.findObject("<http://data.bgs.ac.uk/id/Geochronology/DivisionList/CAA>","0",sparkSession);

        //?s p1 o1
        FindBasicGraphPatterns.findSubject("<http://www.w3.org/2004/02/skos/core#OrderedCollection>","0",sparkSession);

        //?s p1 ?o
        FindBasicGraphPatterns.findSubjectObject("0",sparkSession);

        RdfJoins.findSubjectSubjectJoin("follows","likes","b","i1",sparkSession);

        RdfJoins.findObjectObjectJoin("follows","follows","a","c",sparkSession);



    }


//    /**
//     * Working with triples (s,p,o)
//     * when triples in verticalPartitioning (VP)
//     * s1 p1 ?o
//     * @param subject
//     * @param predicate
//     */
//    public static void findObject(String subject, String predicate,SparkSession sparkSession){
//        //The predicate will tell us the file that we must take
//        //Φορτώνουμε το αρχειο σε ένα Dataset
//        Dataset<Row> df = sparkSession.read().csv(inputPath+predicate+".csv");
//
//        df.createOrReplaceTempView("tableName");
//        //Κάνουμε προβολή των δεδομένων
//        sparkSession.sql("SELECT _c1 as object " +
//                "FROM tableName " +
//                "where _c0='"+subject+"'").show();
//    }


//    /**
//     * Working with triples (s,p,o)
//     * when triples in verticalPartitioning (VP)
//     * ?s p1 o1
//     * @param object
//     * @param predicate
//     */
//    public static void findSubject(String object, String predicate,SparkSession sparkSession){
//        //The predicate will tell us the file that we must take
//        //Φορτώνουμε το αρχειο σε ένα Dataset
//        Dataset<Row> df = sparkSession.read().csv(inputPath+predicate+".csv");
//
//        df.createOrReplaceTempView("tableName");
//        //Κάνουμε προβολή των δεδομένων
//        sparkSession.sql("SELECT _c0 as subject " +
//                "FROM tableName " +
//                "where _c1='"+object+"'").show();
//    }


//    /**
//     * Working with triples (s,p,o)
//     * when triples in verticalPartitioning (VP)
//     * ?s p1 ?o
//     * @param predicate
//     */
//    public static void findSubjectObject( String predicate,SparkSession sparkSession){
//        //The predicate will tell us the file that we must take
//        //Φορτώνουμε το αρχειο σε ένα Dataset
//        Dataset<Row> df = sparkSession.read().csv(inputPath+predicate+".csv");
//
//        df.createOrReplaceTempView("tableName");
//        //Κάνουμε προβολή των δεδομένων
//        sparkSession.sql("SELECT _c0 as subject , _c1 as object " +
//                "FROM tableName ").show();
//    }

}
