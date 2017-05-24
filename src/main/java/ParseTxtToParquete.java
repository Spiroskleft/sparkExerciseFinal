//import model.DataType;
//import org.apache.hadoop.fs.Path;
//import org.semanticweb.yars.nx.Node;
//import org.semanticweb.yars.nx.parser.NxParser;
//
//import java.io.*;
//
//import java.util.*;
//import java.util.regex.Pattern;
//
//
//import parquet.Log;
//import parquet.Preconditions;
//import parquet.column.page.PageReadStore;
//import parquet.example.data.Group;
//import parquet.example.data.simple.convert.GroupRecordConverter;
//import parquet.hadoop.ParquetFileReader;
//import parquet.hadoop.ParquetReader;
//import parquet.hadoop.example.GroupReadSupport;
//import parquet.hadoop.metadata.ParquetMetadata;
//import parquet.io.ColumnIOFactory;
//import parquet.io.MessageColumnIO;
//import parquet.io.RecordReader;
//import parquet.schema.MessageType;
//import parquet.schema.MessageTypeParser;
//import utils.ConvertUtils;
//import utils.CsvParquetWriter;
//import utils.Utils;
//
//
///**
// * Created by tsotzo on 8/5/2017.
// *
// * Δοκιμές απο κώδικα ο οποίο μετρέπει σε java ένα csv σε parquet
// */
//public class ParseTxtToParquete {
//
//    public static void main(String[] args) throws Exception {
//
//        File csv = new File("/home/tsotzo/shareFolder/customer.csv");
//        File paq = new File("/home/tsotzo/shareFolder/p1");
//
//        ConvertUtils.convertCsvToParquet(csv,paq);
//    }
//
//}