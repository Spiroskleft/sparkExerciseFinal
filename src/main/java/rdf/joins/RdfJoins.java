package rdf.joins;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.ReadPropertiesFile;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

/**
 * Created by tsotzo on 15/5/2017.
 */
public class RdfJoins {

    //Θέτουμε τις μεταβλητές για το path των αρχείων -> στη μέθοδο ReadFilePath ()
private static String pathForJoinsCSV = "" ;
private static String pathForJoinsParquet = "";


    // Θα πρέπει να του δίνουμε σαν παράμετρο για κάθε μέθοδο το απο πού θα διαβάζει τα predicates και για το αν θα είναι parquet ή csv
    // τα αρχεία αυτά


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
     * @param type
     * @param path
     */

    public static void findSubjectSubjectJoin(String predicate1, String predicate2, String object1, String object2, SparkSession sparkSession, String type , String path) throws IOException {

        Dataset<Row> df1 = null;
        Dataset<Row> df2 = null;

         // Διαβάζουμε από το κατάλληλο path
        ReadFilePath();

        //The predicate will tell us the file that we must take
        //Φορτώνουμε το κάθε αρχείο σε ένα Dataset αναλόγως με το αν είναι csv ή parquet

        if (Objects.equals(type, "csv")) {

            path = pathForJoinsCSV ;
             df1 = sparkSession.read().csv(path + predicate1 + ".csv");
             df2 = sparkSession.read().csv(path + predicate2 + ".csv");

        }
        else if (Objects.equals(type, "parquet")) {

            path = pathForJoinsParquet;
             df1 = sparkSession.read().parquet(path+ predicate1 + ".parquet");
            df2 = sparkSession.read().parquet(path + predicate2 + ".parquet");
        }

        else {
            System.out.println("Wrong File Type as a Parameter, Select 'csv' or 'parquet' ");
        }

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
     * @param type
     *
     */
    public static void findObjectObjectJoin(String predicate1, String predicate2, String subject1, String subject2, SparkSession sparkSession, String type) throws AnalysisException, IOException {

        Dataset<Row> dfa = null;
        Dataset<Row> dfb = null;

        // Διαβάζουμε από το κατάλληλο path
        ReadFilePath();

        //Φορτώνουμε το κάθε αρχείο σε ένα Dataset αναλόγως με το αν είναι csv ή parquet

        if (Objects.equals(type, "csv")) {


            dfa = sparkSession.read().csv(pathForJoinsCSV + predicate1 + ".csv");
            dfb = sparkSession.read().csv(pathForJoinsCSV + predicate2 + ".csv");

        }
        else if (Objects.equals(type, "parquet")) {


            dfa = sparkSession.read().parquet(pathForJoinsParquet+ predicate1 + ".parquet");
            dfb = sparkSession.read().parquet(pathForJoinsParquet + predicate2 + ".parquet");
        }

        else {
            System.out.println("Wrong File Type as a Parameter, Select 'csv' or 'parquet' ");
        }



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
     * @param type

     */
    public static void findObjectSubjectJoin(String predicate1, String predicate2, String subject1, String object2, SparkSession sparkSession, String type) throws AnalysisException, IOException {

        Dataset<Row> df3 = null;
        Dataset<Row> df4 = null;

        // Διαβάζουμε από το κατάλληλο path
        ReadFilePath();

        //Φορτώνουμε το κάθε αρχείο σε ένα Dataset αναλόγως με το αν είναι csv ή parquet

        if (Objects.equals(type, "csv")) {


            df3 = sparkSession.read().csv(pathForJoinsCSV + predicate1 + ".csv");
            df4 = sparkSession.read().csv(pathForJoinsCSV + predicate2 + ".csv");

        }
        else if (Objects.equals(type, "parquet")) {


            df3 = sparkSession.read().parquet(pathForJoinsParquet+ predicate1 + ".parquet");
            df4 = sparkSession.read().parquet(pathForJoinsParquet + predicate2 + ".parquet");
        }

        else {
            System.out.println("Wrong File Type as a Parameter, Select 'csv' or 'parquet' ");
        }

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
    public static void findSubjectObjectJoin(String predicate1, String predicate2,  String object1,String subject2, SparkSession sparkSession, String type) throws AnalysisException, IOException {

        Dataset<Row> df1 = null;
        Dataset<Row> df2 = null;

        // Διαβάζουμε από το κατάλληλο path
        ReadFilePath();

        //Φορτώνουμε το κάθε αρχείο σε ένα Dataset αναλόγως με το αν είναι csv ή parquet

        if (Objects.equals(type, "csv")) {


            df1 = sparkSession.read().csv(pathForJoinsCSV + predicate1 + ".csv");
            df2 = sparkSession.read().csv(pathForJoinsCSV + predicate2 + ".csv");

        }
        else if (Objects.equals(type, "parquet")) {


            df1 = sparkSession.read().parquet(pathForJoinsParquet+ predicate1 + ".parquet");
            df2 = sparkSession.read().parquet(pathForJoinsParquet + predicate2 + ".parquet");
        }

        else {
            System.out.println("Wrong File Type as a Parameter, Select 'csv' or 'parquet' ");
        }



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

    public static void ReadFilePath () throws IOException {
        Properties prop = new Properties();
        InputStream input = null;
        input = RdfJoins.class.getClassLoader().getResourceAsStream("config.properties");

        // load a properties file
        prop.load(input);
        pathForJoinsCSV = prop.getProperty("pathForJoinsCSV");
        pathForJoinsParquet = prop.getProperty("pathForJoinsParquet") ;
    }

}
