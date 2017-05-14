package rdf.basicGraphPatterns;

import javafx.beans.property.ReadOnlyProperty;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import rdf.RDFReading;
//
import utils.ReadPropertiesFile;
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
 */
public static void findSubject(String object,String predicate,SparkSession sparkSession){
        //The predicate will tell us the file that we must take
        //Φορτώνουμε το αρχειο σε ένα Dataset
        Dataset<Row> df=sparkSession.read().csv(ReadPropertiesFile.inputPath+predicate+".csv");

        df.createOrReplaceTempView("tableName");
        //Κάνουμε προβολή των δεδομένων
        sparkSession.sql("SELECT _c0 as subject "+
        "FROM tableName "+
        "where _c1='"+object+"'").show();
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
                Dataset<Row> df = sparkSession.read().csv(ReadPropertiesFile.inputPath+predicate+".csv");

                df.createOrReplaceTempView("tableName");
                //Κάνουμε προβολή των δεδομένων
                sparkSession.sql("SELECT _c1 as object " +
                        "FROM tableName " +
                        "where _c0='"+subject+"'").show();
        }

        /**
         * Working with triples (s,p,o)
         * when triples in verticalPartitioning (VP)
         * ?s p1 ?o
         * @param predicate
         */
        public static void findSubjectObject( String predicate,SparkSession sparkSession){
                //The predicate will tell us the file that we must take
                //Φορτώνουμε το αρχειο σε ένα Dataset
                Dataset<Row> df = sparkSession.read().csv(ReadPropertiesFile.inputPath+predicate+".csv");

                df.createOrReplaceTempView("tableName");
                //Κάνουμε προβολή των δεδομένων
                sparkSession.sql("SELECT _c0 as subject , _c1 as object " +
                        "FROM tableName ").show();
        }





}