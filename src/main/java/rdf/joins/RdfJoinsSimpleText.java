package rdf.joins;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Objects;

import static utils.ReadPropertiesFile.readConfigProperty;

/**
 * Created by Spiroskleft@gmail.com on 26/5/2017.
 */

    // Για τα Joins με Simple Text Files

public class RdfJoinsSimpleText {

    /**
     * Working with triples (s,p,o)
     * when triples in Simple Text File
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
     * @param file1
     * @param file2
     * @param predicate1
     * @param predicate2
     * @param type
     */
    public static void findSubjectSubjectJoin(String file1, String file2, String object1, String object2, String predicate1, String predicate2, SparkSession sparkSession, String type ) throws IOException {

        Dataset<Row> df1 = null;
        Dataset<Row> df2 = null;
        String path = "";
        //The predicate will tell us the file that we must take
        //Φορτώνουμε το κάθε αρχείο σε ένα Dataset αναλόγως με το αν είναι csv ή parquet
        if (Objects.equals(type, "csv")) {

            df1 = sparkSession.read().csv(readConfigProperty("pathForJoinsCSVSimpleText") + file1 + ".csv");
            df2 = sparkSession.read().csv(readConfigProperty("pathForJoinsCSVSimpleText") + file2 + ".csv");
        } else if (Objects.equals(type, "parquet")) {

            df1 = sparkSession.read().parquet(readConfigProperty("pathForJoinsParquetSimpleText") + file1 + ".parquet");
            df2 = sparkSession.read().parquet(readConfigProperty("pathForJoinsParquetSimpleText") + file2 + ".parquet");
        } else {
            System.out.println("Wrong File Type as a Parameter, Select 'csv' or 'parquet' ");
        }

        df1.createOrReplaceTempView("tableName1");
        df2.createOrReplaceTempView("tableName2");
        //Κάνουμε προβολή των δεδομένων
        System.out.println("-------------------SubjectSubject----------------------------");


        // Άν έχουμε Simple Text File
        // Φορτώνουμε ξανά το ίδιο αρχείο και σαν file1 και σαν file2 οπότε το ερώτημα γίνεται:

        sparkSession.sql("SELECT distinct tableName1._c0 as subject0" +
                " FROM tableName1 , tableName2 " +
                " where tableName1._c2 in ('" + object1 + "') and tableName2._c2 in ('"
                + object2 +"') and tableName1._c0=tableName2._c0"
                + " and tableName1._c1 in ('"+ predicate1 +"')"
                + " and tableName2._c1 in ('"+ predicate2 +"')").show();


    }

    /**
     * Working with triples (s,p,o)
     * when triples in Simple Text File
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
     * @param type
     * @param file1
     * @param file2
     *
     */
    public static void findObjectObjectJoin(String file1, String file2 , String subject1, String subject2,String predicate1, String predicate2, SparkSession sparkSession, String type) throws AnalysisException, IOException {

        Dataset<Row> dfa = null;
        Dataset<Row> dfb = null;
        //Φορτώνουμε το κάθε αρχείο σε ένα Dataset αναλόγως με το αν είναι csv ή parquet
        if (Objects.equals(type, "csv")) {
            dfa = sparkSession.read().csv(readConfigProperty("pathForJoinsCSVSimpleText") + file1 + ".csv");
            dfb = sparkSession.read().csv(readConfigProperty("pathForJoinsCSVSimpleText") + file2 + ".csv");
        }
        else if (Objects.equals(type, "parquet")) {
            dfa = sparkSession.read().parquet(readConfigProperty("pathForJoinsParquetSimpleText") + file1 + ".parquet");
            dfb = sparkSession.read().parquet(readConfigProperty("pathForJoinsParquetSimpleText") + file2 + ".parquet");
        }
        else {
            System.out.println("Wrong File Type as a Parameter, Select 'csv' or 'parquet' ");
        }

        //Φτιάχνουμε τα TempViews
        dfa.createOrReplaceTempView("tableName1");
        dfb.createOrReplaceTempView("tableName2");

        //Κάνουμε προβολή των δεδομένων
        System.out.println("-------------------ObjectObject----------------------------");
        sparkSession.sql("SELECT distinct tableName1._c2 as object1" +
                " FROM tableName1 , tableName2 " +
                " where tableName1._c0 in ('" + subject1 + "') and tableName2._c0 in ('"
                + subject2 +"')"
                + " and tableName1._c2=tableName2._c2"
                + " and tableName1._c1 in ('"+ predicate1 +"')"
                + " and tableName2._c1 in ('"+ predicate2 +"')").show();

    }
    /**
     * Working with triples (s,p,o)
     * when triples in Simple Text File
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
     * @param file1
     * @param file2
     * @param type
     */
    public static void findObjectSubjectJoin(String file1, String file2, String subject1, String object2, String predicate1, String predicate2, SparkSession sparkSession, String type) throws AnalysisException, IOException {

        Dataset<Row> df3 = null;
        Dataset<Row> df4 = null;

        //Φορτώνουμε το κάθε αρχείο σε ένα Dataset αναλόγως με το αν είναι csv ή parquet
        if (Objects.equals(type, "csv")) {
            df3 = sparkSession.read().csv(readConfigProperty("pathForJoinsCSVSimpleText") + file1 + ".csv");
            df4 = sparkSession.read().csv(readConfigProperty("pathForJoinsCSVSimpleText") + file2 + ".csv");
        }
        else if (Objects.equals(type, "parquet")) {
            df3 = sparkSession.read().parquet(readConfigProperty("pathForJoinsParquetSimpleText")+ file1 + ".parquet");
            df4 = sparkSession.read().parquet(readConfigProperty("pathForJoinsParquetSimpleText") + file2 + ".parquet");
        }
        else {
            System.out.println("Wrong File Type as a Parameter, Select 'csv' or 'parquet' ");
        }

        //Φτιάχνουμε τα TempViews
        df3.createOrReplaceTempView("tableName1");
        df4.createOrReplaceTempView("tableName2");

        //Κάνουμε προβολή των δεδομένων
        System.out.println("-------------------ObjectSubject----------------------------");
        sparkSession.sql("SELECT distinct tableName1._c2 as object1" +
                " FROM tableName1 , tableName2 " +
                " where tableName1._c0='" + subject1 + "'" +
                " and tableName1._c1='" + predicate1 + "'" +
                " and tableName2._c1='" + predicate2 + "'" +
                " and tableName1._c2=tableName2._c0" +
                " and tableName2._c2='" + object2 + "'").show();
    }
    /**
     * Working with triples (s,p,o)
     * when triples in Simple Text File
     * Join subject-object (SO)
     * ?s p1 o1
     * s2 p2 ?o
     * Απαντά στο ερώτημα πχ με τους follows
     * Ποιοι ακολουθούν τον ?s και ο ?s likes s1
     *
     * @param subject2
     * @param object1
     * @param predicate1
     * @param predicate2
     * @param file1
     * @param file2
     * @param type
     */
    public static void findSubjectObjectJoin(String file1, String file2 ,String subject2, String object1, String predicate1, String predicate2,  SparkSession sparkSession, String type) throws AnalysisException, IOException {
//
        Dataset<Row> df1 = null;
        Dataset<Row> df2 = null;

        //Φορτώνουμε το κάθε αρχείο σε ένα Dataset αναλόγως με το αν είναι csv ή parquet
        if (Objects.equals(type, "csv")) {
            df1 = sparkSession.read().csv(readConfigProperty("pathForJoinsCSVSimpleText") + file1 + ".csv");
            df2 = sparkSession.read().csv(readConfigProperty("pathForJoinsCSVSimpleText") + file2 + ".csv");
        }
        else if (Objects.equals(type, "parquet")) {
            df1 = sparkSession.read().parquet(readConfigProperty("pathForJoinsParquetSimpleText")+ file1 + ".parquet");
            df2 = sparkSession.read().parquet(readConfigProperty("pathForJoinsParquetSimpleText") + file2 + ".parquet");
        }
        else {
            System.out.println("Wrong File Type as a Parameter, Select 'csv' or 'parquet' ");
        }

        //Φτιάχνουμε τα TempViews
        df1.createOrReplaceTempView("tableName1");
        df2.createOrReplaceTempView("tableName2");

        //Κάνουμε προβολή των δεδομένων
        System.out.println("-------------------SubjectObject----------------------------");
        sparkSession.sql("SELECT distinct tableName1._c0 as subject1,tableName2._c2 as object2" +
                " FROM tableName1 , tableName2 " +
                " where tableName1._c0=tableName2._c2" +
                " and tableName1._c1='" + predicate1 + "'" +
                " and tableName2._c1='" + predicate2 + "'" +
                " and tableName1._c2='" + object1 + "'" +
                " and tableName1._c0=tableName2._c2" +
                " and tableName2._c0='" + subject2 + "'").show();
    }

    // End Of Simple Text Joins
}
