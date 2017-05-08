import Model.DataType;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.NxParser;

import java.io.*;
import java.util.*;

/**
 * Created by tsotzo on 8/5/2017.
 */
public class JavaParseTxt {


    public static void main(String[] args) throws FileNotFoundException {




        //Είναι το αρχείο το οποίο θέλεμε να εξετάσουμε
//        FileInputStream is = new FileInputStream("/home/tsotzo/Desktop/RDF/police.nt");
        FileInputStream is = new FileInputStream("/home/tsotzo/Desktop/RDF/Geochronology_DivisionList.nt");

        //Χρησιμοποιούμε έναν κατάλληλο Parser
        //https://github.com/nxparser/nxparser
        NxParser nxp = new NxParser();
        nxp.parse(is);

        //Είναι η λίστ αμε τα DataType
        List<DataType> list = new ArrayList<>();
        //Ειναι η λίστα για τα τtable
        List<String> tableList = new ArrayList<>();


        for (Node[] nx : nxp) {
            DataType data = new DataType();
            data.setSubject(nx[0].toString());
            data.setTable(nx[1].toString());
            data.setObject(nx[2].toString());

            list.add(data);


            tableList.add(nx[1].toString());
        }
        System.out.println(list.size());

//        List<String> gasList = // create list with duplicates...
        //Βάζω τα table name σε ένα Set για να κόψω τα διπλότυπα
        Set<String> tableSet = new HashSet<String>(tableList);
        System.out.println("tableSet count: " + tableSet.size());

        //Φτιάχνω και ενα map το οποίο θα ειναι to dictionary για να μπορώ να κάνω τα αρχεία με νούμερα
        Map<String,Integer> dictionary = new HashMap<>();

        //Ειναι τα map για να ελέγχω τί έχω γράψει
        Map<String, String> tableMap = new HashMap<>();

        //Γεμίζω το MAP
        List<String> t = new ArrayList<>(tableSet);
        for (int i = 0; i < tableSet.size(); i++) {
            tableMap.put(t.get(i), null);
            dictionary.put(t.get(i),i);
        }
        System.out.println(tableMap.size());


        //Διατρέχω την λίστα μου για να φτιάξω τα αρχεία.
        //Η λογική είναι ότι θα διατρέχω την λίστα μου και κάθε φορά θα κοιτάω με το map άμα υπάρχει
        //το FileoutputStream .Άμα δεν υπάρχει θα το δημιουργώ και θα βάζω μέσα σε αυτό τα στοιχεία που θέλω.
        //Άμα υπάρχει απλά θα το κάνω append.

        //Εδώ ειναι η list κανονικα
        for (int i = 0; i < list.size(); i++) {
//            for (int i = 0; i < 50; i++) {
            String fileName = "";
            //Αν στο map το όνομα του αρχείο ειναι κενο
            if (tableMap.get(list.get(i).getTable()) ==null) {
                //fileName = t.get(i).toString();
                System.out.println("");
                //Βάζω σαν όνομα του αρχείου απο το Dictionary
                fileName = String.valueOf((dictionary.get(list.get(i).getTable())));

                //Ενγμερώνω το tableMap
                tableMap.put(list.get(i).getTable(),fileName);
            }else {
                fileName = String.valueOf((dictionary.get(list.get(i).getTable())));
            }



                String content = list.get(i).getSubject() + "," + list.get(i).getObject()+"\n";
//            FileOutputStream fop;
            FileOutputStream fop = null;
                try{
                    File file = new File("/home/tsotzo/Desktop/RDF/temp/"+fileName+".csv");
                    fop = new FileOutputStream(file,true);
                    // if file doesn't exists, then create it
                    if (!file.exists()) {
                        file.createNewFile();
                    }

                    // get the content in bytes
                    byte[] contentInBytes = content.getBytes();


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
                        System.out.println("Done");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
        }

        }


}
