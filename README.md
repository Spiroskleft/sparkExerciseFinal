# Spark RDF Transformations
---------------------------------
This project works with Spark Cluster with Hadoop Distributed File System ( HDFS ). It also makes  data transformation with RDF format .

# **Getting Started**
----------------------------------
To get started you must clone this project and build it with maven. After that you must copy the jar file into $SPARK_HOME folder and run the spark_submit command.


# **Prerequisites**
----------------------------------
You must have installed HDFS cluster and also Spark Cluster to make this work.
You can find instructions how to build your clusters in the following link 
[HDFS and Spark Cluster](https://docs.google.com/document/d/19dAN-7y6NcJkq2S_PjYv9bWLHrfMwqkQ69zZHbPZ4vw/pub) but this guide is in Greek.

The code is written in Java.

# **Installing**
----------
Clone and build the project. You must have maven already installed.
After that copy the two configuration files , [config.properties](sparkExerciseFinal/src/main/resources/config.properties) and [run.properties](sparkExerciseFinal/src/main/resources/run.properties)  in the following path

    /usr/lib/spark/conf/appConf/

 With [run.properties](sparkExerciseFinal/src/main/resources/run.properties) you can specify what do you want to do and with [config.properties](sparkExerciseFinal/src/main/resources/config.properties) you can specify the paths and folders where the data are.
In these files there are comments which will guide you though, but these are in Greek too. 

After the built you must copy the Jar file into the $SPARK_HOME folder. Our \$SPARK_HOME folder is 

    /usr/lib/spark/
If you follow our instructions for the cluster you should have the same \$SPARK_HOME folder.

After that, you can run the spark_submit command  

    ./spark-submit --master spark://83.212.100.21:7077 --class rdf.RDFReading   sparkExerciseFinal10.jar 

In *- -master* parameter  you must specify the ips of your's spark master node and you can find this in spark's master WEB UI , which is in master ip and in port 8080.

![enter image description here](https://github.com/tsotzolas/Photos/blob/master/SparkExercise/1.png?raw=true)


In  *- - class parameter*   you must put *- -class rdf.RDFReading* which is the name of the main class in our Project.

And the last parameter is the name of the jar file that you copied before into   \$SPARK_HOME folder.


## **What you can do with this code** ##


----------

 - Transform your  RDF Dataset in [Vertical Partitioning](http://cs-www.cs.yale.edu/homes/dna/abadirdf.pdf) format and save the output into HDFS optionally.
 - Transform CSV files into Parquet format.
 - Make Base Graph Pattern queries in CSV  files.
 - Make Base Graph Pattern queries in Vertical Partitioning  files.
 - Make Base Graph Pattern queries in Parquet format  files.
 - Make Joins queries with two tables in CSV  files.
 - Make Joins queries with two tables in Vertical Partitioning  files.
 - Make Joins queries with two tables in Parquet format  files.
 
 

# **Authors**
--------------------------------
 - **Tsotzolas George**
 - **Kleftakis Spiros**
 
See also the list of [contributors](https://github.com/tsotzolas/sparkExerciseFinal/graphs/contributors) who participated in this project.


# **Built With**
----------------------------------

[Maven](https://maven.apache.org/) - Dependency Management
