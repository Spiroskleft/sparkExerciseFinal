package Model;

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
 */
public class HdfsReader {


        public static void main(String[] args) throws IOException {
            String uri = args[0];
            InputStream in = null;
            Path pt = new Path(uri);
            Configuration myConf = new Configuration();
            Path outputPath = new Path(args[1]);

            myConf.set("fs.defaultFS","hdfs://localhost:9000");
            FileSystem fSystem = FileSystem.get(URI.create(uri),myConf);
            OutputStream os = fSystem.create(outputPath);
            try{
                InputStream is = new BufferedInputStream(new FileInputStream(uri));
                IOUtils.copyBytes(is, os, 4096, false);
            }
            catch(IOException e){
                e.printStackTrace();
            }
            finally{
                IOUtils.closeStream(in);
            }
        }

}
