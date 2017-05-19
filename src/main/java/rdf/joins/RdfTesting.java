package rdf.joins;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

/**
 * Created by tsotzo on 15/5/2017.
 */
public class RdfTesting {
    /**
     *
     * @param predicate1
     */
    public static void testParquet(String predicate1,  SparkSession sparkSession) throws IOException {
        //The predicate will tell us the file that we must take
        //Φορτώνουμε το κάθε αρχείο σε ένα Dataset
        Dataset<Row> df1 = sparkSession.read().csv("/usr/lib/spark/bin/RDF/RDF/" + predicate1 + ".csv");


        df1.write().parquet("/home/user/test/a");


//        df1.createOrReplaceTempView("tableName1");
//        //Κάνουμε προβολή των δεδομένων
//        System.out.println("-------------------SubjectSubject----------------------------");
//
//        sparkSession.sql("SELECT distinct tableName1._c0 as subject0" +
//                " FROM tableName1 , tableName2 " +
//                " where tableName1._c1='" + object1 + "'" +
//                " and tableName1._c0=tableName2._c0" +
//                " and tableName2._c1='" + object2 + "'").show();
//    }


    }
}
