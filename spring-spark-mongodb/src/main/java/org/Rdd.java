package org;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;


//start master
//./start-master.sh

// start slave 
//./start-slave.sh spark://localhost:7077  --cores 1 --memory 1g


public class Rdd {
  public static void main(String[] args) {
    String logFile = "/Users/bigdinosaur/Desktop/datarepo/part-00000"; // Should be some file on your system
    //SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory", "2g");
 SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("spark://localhost:7077").set("spark.driver.cores", "1");
 
  // SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("spark://linus:7077");

 
 

    
    
    JavaSparkContext sc = new JavaSparkContext(conf);
    sc.addJar("/Users/neparica/git/springsprakmangodb/spring-spark-mongodb/target/spring-spark-mongodb-0.0.1-SNAPSHOT.jar");
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    long numAs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("BigDinosaur"); }
    }).count();

    long numBs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("BigDinosaur"); }
    }).count();

    
    
    
    
    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
  }
}