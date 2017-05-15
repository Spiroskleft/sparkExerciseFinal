package rdf.joins;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.ReadPropertiesFile;

/**
 * Created by tsotzo on 15/5/2017.
 */
public class RdfJoins {


    /**
     * Working with triples (s,p,o)
     * when triples in verticalPartitioning (VP)
     * ?s p1 o1
     *
     * @param object
     * @param predicate
     */
    public static void findSubjectSubjectJoin(String object, String predicate, SparkSession sparkSession) {
        //The predicate will tell us the file that we must take
        //Φορτώνουμε το αρχειο σε ένα Dataset
        Dataset<Row> df = sparkSession.read().csv(ReadPropertiesFile.inputPath + predicate + ".csv");

        df.createOrReplaceTempView("tableName");
        //Κάνουμε προβολή των δεδομένων
        sparkSession.sql("SELECT _c0 as subject " +
                "FROM tableName " +
                "where _c1='" + object + "'").show();
    }


    /**
     * Working with triples (s,p,o)
     * when triples in verticalPartitioning (VP)
     * ?s p1 o1
     *
     * @param object
     * @param predicate
     */
    public static void findObjectSubjectJoin(String object, String predicate, SparkSession sparkSession) {
        //The predicate will tell us the file that we must take
        //Φορτώνουμε το αρχειο σε ένα Dataset
        Dataset<Row> df = sparkSession.read().csv(ReadPropertiesFile.inputPath + predicate + ".csv");

        df.createOrReplaceTempView("tableName");
        //Κάνουμε προβολή των δεδομένων
        sparkSession.sql("SELECT _c0 as subject " +
                "FROM tableName " +
                "where _c1='" + object + "'").show();
    }









}
