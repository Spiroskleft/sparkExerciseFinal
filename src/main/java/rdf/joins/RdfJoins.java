package rdf.joins;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.ReadPropertiesFile;

/**
 * Created by tsotzo on 15/5/2017.
 */
public class RdfJoins {
    //Πέρνουμε το path που είναι ο φάκελος των δεδομένων απο το properties file
    private final static  String  INPUT_PATH = ReadPropertiesFile.inputPath;


    /**
     * Working with triples (s,p,o)
     * when triples in verticalPartitioning (VP)
     * ?s p1 o1
     * ?s p2 o2
     * Και στο παράδειγμα των follows και likes
     * p1->folloes
     * p2->likes
     * Ποιος follows τον ο1
     * και του likes τον ο2
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
        System.out.println("-------------------SubjectSubject----------------------------");
        sparkSession.sql("SELECT tableName1._c0 as subject1, tableName1._c1 as subject2" +
                " FROM tableName1, tableName2 " +
                " where tableName1._c0='" + object1 + "' AND tableName2._c0='" + object2 +"'"+
                " AND tableName1._c0 = tableName2._c0").show();
    }


    /**
     * Working with triples (s,p,o)
     * when triples in verticalPartitioning (VP)
     * Join object-object (OO)
     * s1 p1 ?o
     * s2 p1 ?o
     * Απαντά στο ερώτημα πχ με τους follows
     * Ποιους ακολουθά ταυτόχρονα και ο s1 και ο s2 ?
     * @param subject1
     * @param subject2
     * @param predicate1
     * @param predicate2
     */
    public static void findObjectObjectJoin( String predicate1,String predicate2,String subject1,String subject2,SparkSession sparkSession) throws AnalysisException {
        //The predicate will tell us the file that we must take
        //Φορτώνουμε το αρχειο σε ένα Dataset
        Dataset<Row> df1 = sparkSession.read().csv(INPUT_PATH + predicate1 + ".csv");
        Dataset<Row> df2 = sparkSession.read().csv(INPUT_PATH + predicate2 + ".csv");

        df1.createOrReplaceTempView("tableName1");
        df2.createOrReplaceTempView("tableName2");
        //Κάνουμε προβολή των δεδομένων


        System.out.println("-------------------ObjectObject----------------------------");
        sparkSession.sql("SELECT tableName1._c1 as object1" +
                " FROM tableName1 , tableName2 " +
                " where tableName1._c0='" + subject1 + "'"+
                " and tableName1._c1=tableName2._c1"+
                " and tableName2._c0='" + subject2 + "'").show();

    }









}
