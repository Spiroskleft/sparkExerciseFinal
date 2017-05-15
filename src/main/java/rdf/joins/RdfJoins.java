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
     * @param object1
     * @param object2
     * @param predicate1
     * @param predicate2
     */
    public static void findSubjectSubjectJoin(String predicate1, String predicate2, String object1, String object2, SparkSession sparkSession) {
        //The predicate will tell us the file that we must take
        //Φορτώνουμε το αρχειο σε ένα Dataset
        Dataset<Row> df = sparkSession.read().csv(ReadPropertiesFile.inputPath + predicate1 + ".csv");
        Dataset<Row> df2 = sparkSession.read().csv(ReadPropertiesFile.inputPath + predicate2 + ".csv");

        df.createOrReplaceTempView("tableName1");
        df2.createOrReplaceTempView("tableName2");
        //Κάνουμε προβολή των δεδομένων
        sparkSession.sql("SELECT _c0 as subject1, _c1 as subject2" +
                "FROM tableName1, tableName2 " +
                "where tableName1._c0='" + object1 + " AND tableName2._c0=" + object2 +
                " AND tableName1.subject1 = tableName2.subject2" +"'").show();
    }


    /**
     * Working with triples (s,p,o)
     * when triples in verticalPartitioning (VP)
     * ?s p1 o1
     *
     * @param subject1
     * @param subject2
     * @param predicate1
     * @param predicate2
     */
    public static void findObjectObjectJoin(String subject1,String subject2, String predicate1,String predicate2, SparkSession sparkSession) {
        //The predicate will tell us the file that we must take
        //Φορτώνουμε το αρχειο σε ένα Dataset
        Dataset<Row> df1 = sparkSession.read().csv(ReadPropertiesFile.inputPath + predicate1 + ".csv");
        Dataset<Row> df2 = sparkSession.read().csv(ReadPropertiesFile.inputPath + predicate2 + ".csv");

        df1.createOrReplaceTempView("tableName1");
        df2.createOrReplaceTempView("tableName2");
        //Κάνουμε προβολή των δεδομένων



        sparkSession.sql("SELECT _c0 as subject, _c1 as object " +
                " FROM tableName1 , tableName2 " +
                " where tableName1._c1='" + subject1 + "'"+
                " and tableName1._c1=tableName2._c1="+
                " and tableName2._c0='" + subject2 + "'").show();






    }









}
