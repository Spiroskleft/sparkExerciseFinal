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
        Dataset<Row> parquet = sparkSession.read()
                .parquet("/home/user/test/p.parquet");

//        df1.write().parquet("/home/user/test/a");


        parquet.createOrReplaceTempView("tableName1");
        System.out.println("-------------------parquet----------------------------");
//
        sparkSession.sql("SELECT *  FROM tableName1").show();
    }


    public static void outPuttoParquet(String predicate1,  SparkSession sparkSession) throws IOException {
        //Read csv from HDFS
        Dataset<Row> df1 = sparkSession.read().csv("hdfs://master:8020/test/temp11/" + predicate1 + ".csv");

        //Write parquet to HDFS
        df1.write().parquet("hdfs://master:8020/test/temp111");


        df1.createOrReplaceTempView("tableName1");
        System.out.println("-------------------parquet----------------------------");
//
        sparkSession.sql("SELECT *  FROM tableName1").show();

    }





}
