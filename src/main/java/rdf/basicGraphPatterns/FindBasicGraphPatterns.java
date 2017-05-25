package rdf.basicGraphPatterns;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
//
import utils.ReadPropertiesFile;

import java.io.IOException;
import java.util.Objects;

import static org.apache.hadoop.hdfs.server.namenode.ListPathsServlet.df;
//import utils.ReadPropertiesFile.inputPath;

/**
 * Created by tsotzo on 14/5/2017.
 */
public class FindBasicGraphPatterns {


    /**
     * Working with triples (s,p,o)
     * when triples in verticalPartitioning (VP)
     * ?s p1 o1
     *
     * @param object
     * @param predicate
     * @param type
     */
    public static void findSubject(String object, String predicate, SparkSession sparkSession, String type) throws IOException {
        Dataset<Row> df = null;

       if (Objects.equals(type, "csv")) {
           //The predicate will tell us the file that we must take
           //Φορτώνουμε το αρχειο σε ένα Dataset
           df= sparkSession.read().csv(ReadPropertiesFile.readRDFDataInputPath()+ predicate + ".csv");
       }
       else if (Objects.equals(type, "parquet")) {
           //The predicate will tell us the file that we must take
           //Φορτώνουμε το αρχειο σε ένα Dataset
           df= sparkSession.read().parquet(ReadPropertiesFile.readRDFDataInputPath() + predicate + ".parquet");
       }
       else {
           System.out.println("Wrong file type, Select 'csv' or 'parquet' as a parameter");

       }
        df.createOrReplaceTempView("tableName");
        //Κάνουμε προβολή των δεδομένων
        sparkSession.sql("SELECT _c0 as subject " +
                "FROM tableName " +
                "where _c1='" + object + "'").show();
    }


    /**
     * Working with triples (s,p,o)
     * when triples in verticalPartitioning (VP)
     * s1 p1 ?o
     *
     * @param subject
     * @param predicate
     * @param type
     */
    public static void findObject(String subject, String predicate, SparkSession sparkSession, String type) throws IOException {
        Dataset<Row> df = null;

        if (Objects.equals(type, "csv")) {
            //The predicate will tell us the file that we must take
            //Φορτώνουμε το αρχειο σε ένα Dataset
            df= sparkSession.read().csv(ReadPropertiesFile.readRDFDataInputPath()+ predicate + ".csv");
        }
        else if (Objects.equals(type, "parquet")) {
            //The predicate will tell us the file that we must take
            //Φορτώνουμε το αρχειο σε ένα Dataset
            df= sparkSession.read().parquet(ReadPropertiesFile.readRDFDataInputPath() + predicate + ".parquet");
        }
        else {
            System.out.println("Wrong file type, Select 'csv' or 'parquet' as a parameter");

        }

        df.createOrReplaceTempView("tableName");
        //Κάνουμε προβολή των δεδομένων
        sparkSession.sql("SELECT _c1 as object " +
                "FROM tableName " +
                "where _c0='" + subject + "'").show();
    }

    /**
     * Working with triples (s,p,o)
     * when triples in verticalPartitioning (VP)
     * ?s p1 ?o
     *
     * @param predicate
     * @param type
     */
    public static void findSubjectObject(String predicate, SparkSession sparkSession, String type) throws IOException {
        Dataset<Row> df = null;

        if (Objects.equals(type, "csv")) {
            //The predicate will tell us the file that we must take
            //Φορτώνουμε το αρχειο σε ένα Dataset
            df= sparkSession.read().csv(ReadPropertiesFile.readRDFDataInputPath()+ predicate + ".csv");
        }
        else if (Objects.equals(type, "parquet")) {
            //The predicate will tell us the file that we must take
            //Φορτώνουμε το αρχειο σε ένα Dataset
            df= sparkSession.read().parquet(ReadPropertiesFile.readRDFDataInputPath() + predicate + ".parquet");
        }
        else {
            System.out.println("Wrong file type, Select 'csv' or 'parquet' as a parameter");

        }

        df.createOrReplaceTempView("tableName");
        //Κάνουμε προβολή των δεδομένων
        sparkSession.sql("SELECT _c0 as subject , _c1 as object " +
                "FROM tableName ").show();
    }
}