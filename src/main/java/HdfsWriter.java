import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;

public class HdfsWriter {

    public HdfsWriter(){
    }

    /**
     * Created by Spiroskleft@gmail.com on 8/5/2017.
     * Αυτή η μέθοδος αντιγράφει αρχεία απο έναν φάκελο στο τοπικό file system στο HDFS
     * @param inputHDFSpath Είναι το path απο την οποία θα διαβάζει τα αρχεία προς αντιγραφή
     * @param outputHDFSpath Είναι το path στο HDFS στο οποίο θα γράφει τα αρχεία
     * @throws IOException
     */
    public static void writeToHDFS (String inputHDFSpath, String outputHDFSpath)throws IOException {

            // Ορίζουμε το αρχείο στο local file system
            String uri = inputHDFSpath;
            InputStream in = null;
            Path pt = new Path(uri);

            // Ορίζουμε το conf των αρχείων Hdfs
            Configuration myConf = new Configuration();
            Path outputPath = new Path(outputHDFSpath);

            // Ορίζουμε το path του hdfs
            myConf.set("fs.defaultFS","hdfs://master:8020");

            // Δημιουργία του output (του αρχείου Hdfs)
            FileSystem fs = FileSystem.get(myConf);
            fs.copyFromLocalFile(new Path(inputHDFSpath),
                    new Path(outputHDFSpath));
        }
}
