import model.DataType;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;


import java.io.*;
import java.util.*;


/**
 * Created by tsotzo on 8/5/2017.
 */
public class ParseTxtToVP {
    private  static String dataset = "";
    private  static String outputPath = "";
    private  static String dictionaryFileName = "";
    private  static Boolean writeDataToHDFS = false;

    //Για το HDFS
    private static String inputHDFSpath = "";
    private static String outputHDFSpath = "";
    private  static String filesTypes = "";


    public static void main(String[] args) throws Exception {

        //Διαβάσουμε απο το properties file
        Properties prop = new Properties();
        InputStream input = null;
        input = ParseTxtToVP.class.getClassLoader().getResourceAsStream("config.properties");


        // load a properties file
        prop.load(input);

        dataset = prop.getProperty("dataset");
        outputPath = prop.getProperty("outputPath");
        dictionaryFileName = prop.getProperty("dictionaryFileName");
        filesTypes = prop.getProperty("filesTypes");


        inputHDFSpath = prop.getProperty("inputHDFSpath");
        outputHDFSpath = prop.getProperty("outputHDFSpath");
        writeDataToHDFS = Boolean.valueOf(prop.getProperty("writeDataToHDFS"));

        //Είναι το αρχείο το οποίο θέλεμε να εξετάσουμε
        FileInputStream is = new FileInputStream(dataset);

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
            writeInFile(outputPath, content, fileName, filesTypes);

        }
        //Γράφουμε το Map που είναι το Dictionary σε αρχείο
        writeHashMapToCsv(dictionaryMap);

        System.out.println("Done");

        //Άμα έχουμε επιλέξει να γράψουμε το αρχείο στο HDFS το αποθηκεύουμε
        if(writeDataToHDFS) {
            HdfsWriter.writeToHDFS(inputHDFSpath, outputHDFSpath);
        }

    }


    /**
     * Μετατρέπει Ένα Map σε ένα csv
     * και στο τέλος το αποθηκεύει στο HDFS
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

        //Άμα έχουμε επιλέξει να γράψουμε το αρχείο στο HDFS το αποθηκεύουμε
        if(writeDataToHDFS) {
            //Γράφουμε το αρχείο
            writeInFile(outputPath, sb.toString(), dictionaryFileName, filesTypes);
        }
    }


    /**
     * Αποθηκεύει σε αρχεία το περιοχόμενο το οποίο του δίνεις
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