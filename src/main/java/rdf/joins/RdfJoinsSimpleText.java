package rdf.joins;

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

            df1 = sparkSession.read().csv(readConfigProperty("pathForJoinsCSV") + file1 + ".csv");
            df2 = sparkSession.read().csv(readConfigProperty("pathForJoinsCSV") + file2 + ".csv");
        } else if (Objects.equals(type, "parquet")) {

            df1 = sparkSession.read().parquet(readConfigProperty("pathForJoinsParquet") + file1 + ".parquet");
            df2 = sparkSession.read().parquet(readConfigProperty("pathForJoinsParquet") + file2 + ".parquet");
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
                " where tableName1._c2='" + object1 + "'" +
                " and tableName1._c0=tableName2._c0" +
                " where tableName1._c1='" + predicate1 + "'" +
                " where tableName2._c1='" + predicate2 + "'" +
                " and tableName2._c2='" + object2 + "'").show();

    }
}
