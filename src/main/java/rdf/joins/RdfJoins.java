package rdf.joins;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.ReadPropertiesFile;

import java.io.IOException;

/**
 * Created by tsotzo on 15/5/2017.
 */
public class RdfJoins {
    /**
     * Working with triples (s,p,o)
     * when triples in verticalPartitioning (VP)
     * ?s p1 o1
     * ?s p2 o2
     * Και στο παράδειγμα των follows και likes
     * p1->follows
     * p2->likes
     * Ποιος follows τον ο1
     * και του likes τον ο2
     *
     * @param object1
     * @param object2
     * @param predicate1
     * @param predicate2
     */
    public static void findSubjectSubjectJoin(String predicate1, String predicate2, String object1, String object2, SparkSession sparkSession) throws IOException {
        //The predicate will tell us the file that we must take
        //Φορτώνουμε το κάθε αρχείο σε ένα Dataset
        Dataset<Row> df1 = sparkSession.read().csv("hdfs://localhost:9000/exampleWithFollows/example/"+ predicate1 + ".csv");
        Dataset<Row> df2 = sparkSession.read().csv("hdfs://localhost:9000/exampleWithFollows/example/"+ predicate2 + ".csv");

        df1.createOrReplaceTempView("tableName1");
        df2.createOrReplaceTempView("tableName2");
        //Κάνουμε προβολή των δεδομένων
        System.out.println("-------------------SubjectSubject----------------------------");

        sparkSession.sql("SELECT distinct tableName1._c0 as subject0" +
                " FROM tableName1 , tableName2 " +
                " where tableName1._c1='" + object1 + "'" +
                " and tableName1._c0=tableName2._c0" +
                " and tableName2._c1='" + object2 + "'").show();
    }


    /**
     * Working with triples (s,p,o)
     * when triples in verticalPartitioning (VP)
     * Join object-object (OO)
     * s1 p1 ?o
     * s2 p1 ?o
     * Απαντά στο ερώτημα πχ με τους follows
     * Ποιους ακολουθά ταυτόχρονα και ο s1 και ο s2 ?
     *
     * @param subject1
     * @param subject2
     * @param predicate1
     * @param predicate2
     */
    public static void findObjectObjectJoin(String predicate1, String predicate2, String subject1, String subject2, SparkSession sparkSession) throws AnalysisException, IOException {
        //The predicate will tell us the file that we must take
        //Φορτώνουμε το αρχειο σε ένα Dataset
        Dataset<Row> dfa = sparkSession.read().csv("hdfs://localhost:9000/exampleWithFollows/example/"+ predicate1 + ".csv");
        Dataset<Row> dfb = sparkSession.read().csv("hdfs://localhost:9000/exampleWithFollows/example/"+ predicate2 + ".csv");

        dfa.createOrReplaceTempView("tableName1");
        dfb.createOrReplaceTempView("tableName2");
        //Κάνουμε προβολή των δεδομένων


        System.out.println("-------------------ObjectObject----------------------------");
        sparkSession.sql("SELECT distinct tableName1._c1 as object1" +
                " FROM tableName1 , tableName2 " +
                " where tableName1._c0='" + subject1 + "'" +
                " and tableName1._c1=tableName2._c1" +
                " and tableName2._c0='" + subject2 + "'").show();

    }




    /**
     * Working with triples (s,p,o)
     * when triples in verticalPartitioning (VP)
     * Join object-subject (OS)
     * s1 p1 ?o
     * ?s p2 o2
     * Απαντά στο ερώτημα πχ με τους follows
     * Ποιοι ακολουθούν τον ?ο και αυτοί likes ο2 ?
     *
     * @param subject1
     * @param object2
     * @param predicate1
     * @param predicate2
     */
    public static void findObjectSubjectJoin(String predicate1, String predicate2, String subject1, String object2, SparkSession sparkSession) throws AnalysisException, IOException {
        //The predicate will tell us the file that we must take
        //Φορτώνουμε το αρχειο σε ένα Dataset
        Dataset<Row> df3 = sparkSession.read().csv("hdfs://localhost:9000/exampleWithFollows/example/"+ predicate1 + ".csv");
        Dataset<Row> df4 = sparkSession.read().csv("hdfs://localhost:9000/exampleWithFollows/example/" + predicate2 + ".csv");

        df3.createOrReplaceTempView("tableName1");
        df4.createOrReplaceTempView("tableName2");
        //Κάνουμε προβολή των δεδομένων


        System.out.println("-------------------ObjectSubject----------------------------");
        sparkSession.sql("SELECT distinct tableName1._c1 as object1" +
                " FROM tableName1 , tableName2 " +
                " where tableName1._c0='" + subject1 + "'" +
                " and tableName1._c1=tableName2._c0" +
                " and tableName2._c1='" + object2 + "'").show();
    }


    /**
     * Working with triples (s,p,o)
     * when triples in verticalPartitioning (VP)
     * Join object-subject (OS)
     * ?s p2 o2
     * s1 p1 ?o
     * Απαντά στο ερώτημα πχ με τους follows
     * Ποιοι ακολουθούν τον ?s και ο ?s likes s1
     *
     * @param subject2
     * @param object1
     * @param predicate1
     * @param predicate2
     */
    public static void findSubjectObjectJoin(String predicate1, String predicate2,  String object1,String subject2, SparkSession sparkSession) throws AnalysisException, IOException {
        //The predicate will tell us the file that we must take
        //Φορτώνουμε το αρχειο σε ένα Dataset

        Dataset<Row> df1 = sparkSession.read().csv("hdfs://master/test/temp/example/" + predicate1 + ".csv");
        Dataset<Row> df2 = sparkSession.read().csv("hdfs://master/test/temp/example/" + predicate2 + ".csv");

        df1.createOrReplaceTempView("tableName1");
        df2.createOrReplaceTempView("tableName2");
        //Κάνουμε προβολή των δεδομένων


        System.out.println("-------------------SubjectObject----------------------------");
        sparkSession.sql("SELECT distinct tableName1._c0 as object0,tableName2._c1 as subject1" +
                " FROM tableName1 , tableName2 " +
                " where tableName1._c0=tableName2._c1" +
                " and tableName1._c1='" + object1 + "'" +
                " and tableName2._c0='" + subject2 + "'").show();
    }
}
