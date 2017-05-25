package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class HdfsWriter {

    public HdfsWriter() {

    }

    /**
     * Created by Spiroskleft@gmail.com on 8/5/2017.
     * Αυτή η μέθοδος αντιγράφει αρχεία απο έναν φάκελο στο τοπικό file system στο HDFS
     *
     * @param inputHDFSpath  Είναι το path απο την οποία θα διαβάζει τα αρχεία προς αντιγραφή
     * @param outputHDFSpath Είναι το path στο HDFS στο οποίο θα γράφει τα αρχεία
     * @throws IOException
     */
    public static void writeToHDFS(String inputHDFSpath, String outputHDFSpath) throws IOException {


        // Ορίζουμε το αρχείο στο local file system
        String uri = inputHDFSpath;
        InputStream in = null;
        Path pt = new Path(uri);

        // Ορίζουμε το conf των αρχείων Hdfs
        Configuration myConf = new Configuration();
        Path outputPath = new Path(outputHDFSpath);

        // Ορίζουμε το path του hdfs
        myConf.set("fs.defaultFS", "hdfs://master:8020");

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


    /**
     * Αυτή η μέθοδος αντιγράφει αρχεία απο έναν φάκελο στο τοπικό file system στο HDFS
     * @param inputHDFSpath  Είναι το path απο την οποία θα διαβάζει τα αρχεία προς αντιγραφή
     * @param outputHDFSpath Είναι το path στο HDFS στο οποίο θα γράφει τα αρχεία
     * @throws IOException
     */
    public static void renameToHDFS(String inputHDFSpath, String outputHDFSpath) throws IOException {
        // Ορίζουμε το αρχείο στο local file system
        String uri = inputHDFSpath;
        InputStream in = null;
        Path pt = new Path(uri);

        // Ορίζουμε το conf των αρχείων Hdfs
        Configuration myConf = new Configuration();

        // Ορίζουμε το path του hdfs
        myConf.set("fs.defaultFS", "hdfs://master:8020");

        // Δημιουργία του output (του αρχείου Hdfs)

        FileSystem fs = FileSystem.get(myConf);
        fs.mkdirs(new Path("/testP/"));
        FileStatus afs[] = fs.listStatus(new Path("hdfs://master:8020/test/temp111/"));

        for (FileStatus aFile : afs) {
            if (aFile.isDir()) {
                fs.delete(aFile.getPath(), true);
// delete all directories and sub-directories (if any) in the output directory
            } else {
                if (aFile.getPath().getName().contains("_SUCCESS"))
                    fs.delete(aFile.getPath(), true);
// delete all log files and the _SUCCESS file in the output directory
                else {
                    fs.rename(aFile.getPath(), new Path("0.parquet"));
                }
            }

        }
    }
}
