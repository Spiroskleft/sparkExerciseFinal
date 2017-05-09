import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by Spiroskleft@gmail.com on 8/5/2017.
 * Σκοπός αυτής της κλάσης είναι η μετατροπή οποιουδήποτε αρχείου από το local file system σε αρχείο HDFS
 * Για να το τρέξουμε αρκεί να δώσουμε δύο παραμέτρους: arg[0] : Το αρχείο του local file system
 *                                                      arg[1] : Το αρχείο που επιθυμούμε να δημιουργηθεί στο HDFS
 */


public class HdfsWriter {

    public HdfsWriter(){

    }

        public static void writeToHDFS (String inputHDFSpath, String outputHDFSpath)throws IOException {


            // Ορίζουμε το αρχείο στο local file system
            String uri = inputHDFSpath;
            InputStream in = null;
            Path pt = new Path(uri);
            // Ορίζουμε το conf των αρχείων Hdfs
            Configuration myConf = new Configuration();
            Path outputPath = new Path(outputHDFSpath);
            // Ορίζουμε το path του hdfs
            myConf.set("fs.defaultFS","hdfs://localhost:9000");
            // Δημιουργία του output (του αρχείου Hdfs)

            FileSystem fs = FileSystem.get(myConf);
            fs.copyFromLocalFile(new Path(inputHDFSpath),
                    new Path(outputHDFSpath));
//            OutputStream os = fSystem.create(outputPath);
//            try{
//                InputStream is = new BufferedInputStream(new FileInputStream(uri));
//                IOUtils.copyBytes(is, os, 4096, false);
//            }
//            catch(IOException e){
//                e.printStackTrace();
//            }
//            finally{
//                IOUtils.closeStream(in);
//            }
        }

}
