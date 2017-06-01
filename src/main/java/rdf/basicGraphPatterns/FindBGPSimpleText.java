package rdf.basicGraphPatterns;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Objects;

import static utils.ReadPropertiesFile.readConfigProperty;

/**
 * Created by Spiroskleft@gmail.com on 1/6/2017.
 */
public class FindBGPSimpleText {
    private static String outputQueries ="";
    /**
     * Working with triples (s,p,o)
     * when triples in Simple Text File
     * ?s p1 o1
     *
     * @param object
     * @param predicate
     * @param type
     * @param file
     */
    public static void findSubject(String file, String predicate,String object, SparkSession sparkSession, String type) throws IOException {
        Dataset<Row> df = null;

        if (Objects.equals(type, "csv")) {
            //The predicate will tell us the file that we must take
            //Φορτώνουμε το αρχειο σε ένα Dataset
            df= sparkSession.read().csv(readConfigProperty("RDFDataInputPathSimpleText")+ file + ".csv");
        }
        else if (Objects.equals(type, "parquet")) {
            //The predicate will tell us the file that we must take
            //Φορτώνουμε το αρχειο σε ένα Dataset
            df= sparkSession.read().parquet(readConfigProperty("RDFDataInputPathSimpleText" + file + ".parquet"));
        }
        else {
            System.out.println("Wrong file type, Select 'csv' or 'parquet' as a parameter");

        }
        df.createOrReplaceTempView("tableName");
        //Κάνουμε προβολή των δεδομένων
        sparkSession.sql("SELECT _c0 as subject " +
                "FROM tableName " +
                "where _c1='" + predicate + "'" +
                " and _c2='" + object + "'").show();

        // Γράφουμε το query σε output file που έχουμε καθορίσει στο config.properties
//        df.write().text(readConfigProperty("outputBGPSimpleText"));
    }


    /**
     * Working with triples (s,p,o)
     * when triples in Simple Text File
     * s1 p1 ?o
     *
     * @param subject
     * @param predicate
     * @param type
     * @param file
     */
    public static void findObject(String file, String subject, String predicate, SparkSession sparkSession, String type) throws IOException {
        Dataset<Row> df = null;
        if (Objects.equals(type, "csv")) {
            //The predicate will tell us the file that we must take
            //Φορτώνουμε το αρχειο σε ένα Dataset
            df= sparkSession.read().csv(readConfigProperty("RDFDataInputPathSimpleText")+ file + ".csv");
        }
        else if (Objects.equals(type, "parquet")) {
            //The predicate will tell us the file that we must take
            //Φορτώνουμε το αρχειο σε ένα Dataset
            df= sparkSession.read().parquet(readConfigProperty("RDFDataInputPathSimpleText") + file + ".parquet");
        }
        else {
            System.out.println("Wrong file type, Select 'csv' or 'parquet' as a parameter");

        }

        df.createOrReplaceTempView("tableName");
        //Κάνουμε προβολή των δεδομένων
        sparkSession.sql("SELECT _c2 as object " +
                "FROM tableName " +
                "where _c1='" + predicate + "'" +
                " and _c0='" + subject + "'").show();

        // Γράφουμε το query σε output file που έχουμε καθορίσει στο config.properties
//        df.write().text(readConfigProperty("outputBGPSimpleText"));
    }


    /**
     * Working with triples (s,p,o)
     * when triples in Simple Text File
     * s1 ?p o1
     *
     * @param subject
     * @param object
     * @param type
     * @param file
     */
    public static void findpredicate(String file, String subject, String object, SparkSession sparkSession, String type) throws IOException {
        Dataset<Row> df = null;
        if (Objects.equals(type, "csv")) {
            //The predicate will tell us the file that we must take
            //Φορτώνουμε το αρχειο σε ένα Dataset
            df= sparkSession.read().csv(readConfigProperty("RDFDataInputPathSimpleText")+ file + ".csv");
        }
        else if (Objects.equals(type, "parquet")) {
            //The predicate will tell us the file that we must take
            //Φορτώνουμε το αρχειο σε ένα Dataset
            df= sparkSession.read().parquet(readConfigProperty("RDFDataInputPathSimpleText") + file + ".parquet");
        }
        else {
            System.out.println("Wrong file type, Select 'csv' or 'parquet' as a parameter");

        }

        df.createOrReplaceTempView("tableName");
        //Κάνουμε προβολή των δεδομένων
        sparkSession.sql("SELECT _c1 as predicate " +
                "FROM tableName " +
                "where _c2='" + object + "'" +
                " and _c0='" + subject + "'").show();

//        // Γράφουμε το query σε output file που έχουμε καθορίσει στο config.properties
//        df.write().text(readConfigProperty("outputBGPSimpleText"));
    }

    /**
     * Working with triples (s,p,o)
     * when triples in Simple Text File
     * ?s p1 ?o
     *
     * @param predicate
     * @param type
     * @param file
     */
    public static void findSubjectObject(String file,String predicate, SparkSession sparkSession, String type) throws IOException {
        Dataset<Row> df = null;

        if (Objects.equals(type, "csv")) {
            //The predicate will tell us the file that we must take
            //Φορτώνουμε το αρχειο σε ένα Dataset
            df= sparkSession.read().csv(readConfigProperty("RDFDataInputPathSimpleText")+ file + ".csv");
        }
        else if (Objects.equals(type, "parquet")) {
            //The predicate will tell us the file that we must take
            //Φορτώνουμε το αρχειο σε ένα Dataset
            df= sparkSession.read().parquet(readConfigProperty("RDFDataInputPathSimpleText") + file + ".parquet");
        }
        else {
            System.out.println("Wrong file type, Select 'csv' or 'parquet' as a parameter");

        }

        df.createOrReplaceTempView("tableName");
        //Κάνουμε προβολή των δεδομένων
        sparkSession.sql("SELECT _c0 as subject , _c2 as object " +
                "FROM tableName " +
                "where _c1='" + predicate + "'").show();

        // Γράφουμε το query σε output file που έχουμε καθορίσει στο config.properties
//        df.write().text(readConfigProperty("outputBGPSimpleText"));
    }

    /**
     * Working with triples (s,p,o)
     * when triples in Simple Text File
     * ?s ?p o1
     *
     * @param object
     * @param type
     * @param file
     */
    public static void findSubjectPredicate(String file,String object, SparkSession sparkSession, String type) throws IOException {
        Dataset<Row> df = null;

        if (Objects.equals(type, "csv")) {
            //The predicate will tell us the file that we must take
            //Φορτώνουμε το αρχειο σε ένα Dataset
            df= sparkSession.read().csv(readConfigProperty("RDFDataInputPathSimpleText")+ file + ".csv");
        }
        else if (Objects.equals(type, "parquet")) {
            //The predicate will tell us the file that we must take
            //Φορτώνουμε το αρχειο σε ένα Dataset
            df= sparkSession.read().parquet(readConfigProperty("RDFDataInputPathSimpleText") + file + ".parquet");
        }
        else {
            System.out.println("Wrong file type, Select 'csv' or 'parquet' as a parameter");

        }

        df.createOrReplaceTempView("tableName");
        //Κάνουμε προβολή των δεδομένων
        sparkSession.sql("SELECT _c0 as subject , _c1 as predicate " +
                "FROM tableName " +
                "where _c2='" + object + "'").show();

        // Γράφουμε το query σε output file που έχουμε καθορίσει στο config.properties
//        df.write().text(readConfigProperty("outputBGPSimpleText"));
    }
    /**
     * Working with triples (s,p,o)
     * when triples in Simple Text File
     * s1 ?p o?
     *
     * @param subject
     * @param type
     * @param file
     */
    public static void findPredicateObject(String file,String subject, SparkSession sparkSession, String type) throws IOException {
        Dataset<Row> df = null;

        if (Objects.equals(type, "csv")) {
            //The predicate will tell us the file that we must take
            //Φορτώνουμε το αρχειο σε ένα Dataset
            df= sparkSession.read().csv(readConfigProperty("RDFDataInputPathSimpleText")+ file + ".csv");
        }
        else if (Objects.equals(type, "parquet")) {
            //The predicate will tell us the file that we must take
            //Φορτώνουμε το αρχειο σε ένα Dataset
            df= sparkSession.read().parquet(readConfigProperty("RDFDataInputPathSimpleText") + file + ".parquet");
        }
        else {
            System.out.println("Wrong file type, Select 'csv' or 'parquet' as a parameter");

        }

        df.createOrReplaceTempView("tableName");
        //Κάνουμε προβολή των δεδομένων
        sparkSession.sql("SELECT _c1 as predicate , _c2 as object " +
                "FROM tableName " +
                "where _c0='" + subject + "'").show();

        // Γράφουμε το query σε output file που έχουμε καθορίσει στο config.properties
//        df.write().text(readConfigProperty("outputBGPSimpleText"));
    }

}
