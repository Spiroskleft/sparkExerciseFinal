package utils;

import model.DataType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;
import java.io.*;
import java.util.*;

import static utils.ReadPropertiesFile.readConfigProperty;

/**
 * Created by tsotzo on 15/5/2017.
 */
public class RdfTrasformation {

    /**
     * Μετατράπει αρχεία RDF σε Vertical Partitioning
     * Θα πρέπει να γίνουν οι ρυθμήσεις των φακέλων στο config.properties
     * @throws IOException
     */
    public static void  parceTxtToVP() throws IOException {

        //Είναι το αρχείο το οποίο θέλεμε να εξετάσουμε
        FileInputStream is = new FileInputStream(readConfigProperty("dataset"));

        //Χρησιμοποιούμε έναν κατάλληλο Parser
        //https://github.com/nxparser/nxparser
        NxParser nxp = new NxParser();
        nxp.parse(is);

        //Είναι η λίστα με τα DataType
        List<DataType> list = new ArrayList<>();
        //Ειναι η λίστα για τα table
        List<String> tableList = new ArrayList<>();

        for (Node[] nx : nxp) {
            DataType data = new DataType();
            data.setSubject(nx[0].toString());
            data.setTable(nx[1].toString());
            data.setObject(nx[2].toString());

            list.add(data);


            tableList.add(nx[1].toString());
        }
        System.out.println("Dataset size:"+list.size());

        //Βάζω τα table name σε ένα Set για να κόψω τα διπλότυπα
        Set<String> tableSet = new HashSet<String>(tableList);
        System.out.println("Unique table count: " + tableSet.size());

        //Φτιάχνω και ενα map το οποίο θα ειναι to dictionary για να μπορώ να κάνω τα αρχεία με νούμερα
        Map<String, Integer> dictionaryMap = new HashMap<>();

        //Ειναι τα map για να ελέγχω τί έχω γράψει
        Map<String, String> tableMap = new HashMap<>();

        //Γεμίζω το MAP
        List<String> t = new ArrayList<>(tableSet);
        for (int i = 0; i < tableSet.size(); i++) {
            tableMap.put(t.get(i), null);
            dictionaryMap.put(t.get(i), i);
        }

        //Διατρέχω την λίστα μου για να φτιάξω τα αρχεία.
        //Η λογική είναι ότι θα διατρέχω την λίστα μου και κάθε φορά θα κοιτάω με το map άμα υπάρχει
        //το το όνομα του αρχειου .Άμα δεν υπάρχει θα το δημιουργώ και θα βάζω μέσα σε αυτό τα στοιχεία που θέλω.
        //Άμα υπάρχει απλά θα το κάνω append.

        //Εδώ ειναι η list κανονικα
        for (int i = 0; i < list.size(); i++) {
            String fileName = "";
            //Αν στο map το όνομα του αρχείο ειναι κενο
            if (tableMap.get(list.get(i).getTable()) == null) {

                //Βάζω σαν όνομα του αρχείου απο το Dictionary
                fileName = String.valueOf((dictionaryMap.get(list.get(i).getTable())));

                //Ενημερώνω το tableMap
                tableMap.put(list.get(i).getTable(), fileName);
            } else {
                fileName = String.valueOf((dictionaryMap.get(list.get(i).getTable())));
            }

            String content = list.get(i).getSubject() + "," + list.get(i).getObject() + "\n";

            //Γράφω σε αρχείο τα αποτελέσματα
            writeInFile(readConfigProperty("outputPath"), content, fileName, readConfigProperty("filesTypes"));

        }
        //Γράφουμε το Map που είναι το Dictionary σε αρχείο
        try {
            writeHashMapToCsv(dictionaryMap);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Done");

        //Άμα έχουμε επιλέξει να γράψουμε το αρχείο στο HDFS το αποθηκεύουμε
        if(Boolean.valueOf(readConfigProperty("writeDataToHDFS"))) {
            HdfsWriter.writeToHDFS(readConfigProperty("inputHDFSpath"), readConfigProperty("outputHDFSpath"));
        }

    }


    /**
     * Μετατράπει αρχεία plain Text σε CSV
     * Θα πρέπει να γίνουν οι ρυθμήσεις των φακέλων στο config.properties
     * @throws IOException
     */
    public static void  parceTxtToCSV() throws IOException {

        //Είναι το αρχείο το οποίο θέλεμε να εξετάσουμε
        FileInputStream is = new FileInputStream(readConfigProperty("dataset"));

        //Χρησιμοποιούμε έναν κατάλληλο Parser
        //https://github.com/nxparser/nxparser
        NxParser nxp = new NxParser();
        nxp.parse(is);

        //Είναι η λίστα με τα DataType
        List<DataType> list = new ArrayList<>();
        //Ειναι η λίστα για τα table
        List<String> tableList = new ArrayList<>();

        for (Node[] nx : nxp) {
            DataType data = new DataType();
            data.setSubject(nx[0].toString());
            data.setTable(nx[1].toString());
            data.setObject(nx[2].toString());

            list.add(data);

            System.out.println("--------------------------------> List Added");
           // tableList.add(nx[1].toString());
        }
        System.out.println("Dataset size:"+list.size());


        //Εδώ ειναι η list κανονικα
        for (int i = 0; i < list.size(); i++) {
            String fileName = is.toString();
            String content = list.get(i).getSubject() + "," + list.get(i).getTable() + "," + list.get(i).getObject() + "\n";

            //Γράφω σε αρχείο τα αποτελέσματα
            writeInFile(readConfigProperty("outputPath"), content, fileName, readConfigProperty("filesTypes"));

        }

        System.out.println("Done");

        //Άμα έχουμε επιλέξει να γράψουμε το αρχείο στο HDFS το αποθηκεύουμε
        if(Boolean.valueOf(readConfigProperty("writeDataToHDFS"))) {
            HdfsWriter.writeToHDFS(readConfigProperty("inputHDFSpath"), readConfigProperty("outputHDFSpath"));
        }
    }


    /**
     * Μετατρέπει ένα CSV file σε μορφή Parquet
     * @param predicate1 είναι το όνομα του CSV αρχείου
     * @param inputCSVPath είναι το path όπου είναι το CSV αρχείο
     * @param outputParquetPath είναι το path όπου θέλουμε να αποθηκεύσουμε το Parquet αρχείο
     * @param sparkSession Είναι το session του Spark το οποίο έχουμε
     * @throws IOException
     */
    public static void tranformCSVtoParquet(String predicate1,String inputCSVPath,String  outputParquetPath, SparkSession sparkSession) throws IOException {
        //Read csv from HDFS
        Dataset<Row> df1 = sparkSession.read().csv(inputCSVPath + predicate1 + ".csv");

        //Write parquet to HDFS
        df1.write().parquet(outputParquetPath + predicate1);


        // Ορίζουμε το conf των αρχείων Hdfs
        Configuration myConf = new Configuration();

        // Ορίζουμε το path του hdfs
        myConf.set("fs.defaultFS", readConfigProperty("HDFSMasterConf"));

        FileSystem fs = FileSystem.get(myConf);
        FileStatus afs[] = fs.listStatus(new Path(outputParquetPath+predicate1));
        for (FileStatus aFile : afs) {
//            if (aFile.isDir()) {
//                fs.delete(aFile.getPath(), true);
//                // delete all directories and sub-directories (if any) in the output directory
//            } else {
            //Σβήνουμε το _SUCCESS αρχείο
                if (aFile.getPath().getName().contains("_SUCCESS")) {
                    System.out.println("-------------------delete----------------------------");
                    fs.delete(aFile.getPath(), true);
                }
                    // Μετονομάσουμε το αρχείο part-00000... σε δικό μας όνομα
                else if ((aFile.getPath().getName().contains("part-00000"))) {
                    System.out.println("-------------------rename----------------------------");
                    fs.rename(aFile.getPath(), new Path(outputParquetPath+predicate1 +"/"+predicate1 + ".parquet"));
                }else
                    System.out.println("------------------Nothing---------------");

            }


//        Dataset<Row> sqlDF =sparkSession.sql("SELECT * FROM parquet.`"+outputParquetPath+predicate1+"/"+predicate1 + ".parquet"+"`");
//        sqlDF.show();
        }

    /**
     * Μετατρέπει Ένα Map σε ένα csv
     * και άμα το έχουμε επιλέξει το αποθηκευσει σε αρχείο
     *
     * @param map
     * @throws Exception
     */
    public static void writeHashMapToCsv(Map<String, Integer> map) throws Exception {

        StringBuilder sb = new StringBuilder();

        StringWriter output = new StringWriter();
        try  {
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                sb.append(entry.getValue().toString());
                sb.append(",");
                sb.append(entry.getKey().toString());
                sb.append("\n");
            }
        } catch (Exception e ){
            e.printStackTrace();
        }
            //Γράφουμε το αρχείο
            writeInFile(readConfigProperty("outputPath"), sb.toString(), readConfigProperty("dictionaryFileName"), readConfigProperty("filesTypes"));
    }


    /**
     * Αποθηκεύει σε αρχεία το περιεχόμενο το οποίο του δίνεις
     *
     * @param filepath Είναι η παράμετρος όπου του ορίζεις το path που θές να αποθηκεύσεις τα αρχεία
     * @param content  Είναι το περιεχόμενο το οποίο θές να έχει το αρχείο
     * @param fileName Είναι το όνομα το οποίο θές να έχει το αρχείο
     * @param fileType Είναι το τύπος του αρχείου που θές
     */
    public static void writeInFile(String filepath, String content, String fileName, String fileType) {
        FileOutputStream fop = null;
        String filePathType = filepath + fileName + "." + fileType;
        try {
            File file = new File(filePathType);
            fop = new FileOutputStream(file, true);
            // if file doesn't exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }

            // get the content in bytes
            byte[] contentInBytes = content.getBytes("UTF-8");
            if (fop != null) {
                fop.write(contentInBytes);

                fop.flush();
                fop.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fop != null) {
                    fop.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}