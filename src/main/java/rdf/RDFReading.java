package rdf;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

import utils.RdfTrasformation;

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



        //Create the Spark session
        sparkSession = SparkSession.builder().appName("RDFreader").getOrCreate();


        String inputCSVPath = "/usr/lib/spark/bin/RDF/RDF/";
        String outputParquetPath = "hdfs://master:8020/test/temp11/";
        RdfTrasformation.tranformCSVtoParquet("0",inputCSVPath,outputParquetPath,sparkSession);


        //s1 p1 ?o
//        FindBasicGraphPatterns.findObject("<http://data.bgs.ac.uk/id/Geochronology/DivisionList/CAA>","0",sparkSession);

        //?s p1 o1
//        FindBasicGraphPatterns.findSubject("<http://www.w3.org/2004/02/skos/core#OrderedCollection>","0",sparkSession);

        //?s p1 ?o
//        FindBasicGraphPatterns.findSubjectObject("0",sparkSession);

        /***********************Joins***************/

        //?s p1 o1
        //?s p2 o2
        //Πρέπει να βγάλει a
//        RdfJoins.findSubjectSubjectJoin("follows","likes","b","i1",sparkSession);


        //s1 p1 ?o
        //s2 p1 ?o
        //Πρέπει να βγάλει a
//        RdfJoins.findObjectObjectJoin("follows","follows","a","c",sparkSession);

        //s1 p1 ?o
        //?s p2 o2
        //Πρέπει να βγάλει c
//        RdfJoins.findObjectSubjectJoin("follows","likes","b","i2",sparkSession);


        //?s p2 o2
        //s1 p1 ?o
        //Πρέπει να βγάλει c
//        RdfJoins.findSubjectObjectJoin("likes","follows","i2","b",sparkSession);


//        RdfTesting.testParquet("likes",sparkSession);

    }

}
