package org;
//start server
// bin/mongod
// start mongo shell
// bin/mongo
//show dbs
// use db
// show collections

import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.BSONObject;

import com.mongodb.hadoop.MongoInputFormat;
	public class App {
	  public static void main(String[] args) {
	    Configuration mongodbConfig = new Configuration();
	    mongodbConfig.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
	    mongodbConfig.set("mongo.input.uri", "mongodb://localhost:27017/db.db");
	    SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple Application"); //see here
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    JavaPairRDD<Object, BSONObject> documents = sc.newAPIHadoopRDD(
	        mongodbConfig,            // Configuration
	        MongoInputFormat.class,   // InputFormat: read from a live cluster.
	        Object.class,             // Key class
	        BSONObject.class          // Value class
	    );
	List n=	  documents.collect();
	Iterator v=(Iterator) n.iterator();
	while(v.hasNext()){
		System.out.println(v.next());
	}
	
	 
	
	
	
	  }
	  
	 
	}