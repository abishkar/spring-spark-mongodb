package com.stream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

//nc -l localhost 9999
public class SparkStreaming {
	private static final String HOST = "127.0.0.1";
	private static final int PORT = 10007;

	public static void main(String[] args) {
		// Configure and initialize the SparkStreamingContext
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("VerySimpleStreamingApp");
		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(12));
		Logger.getRootLogger().setLevel(Level.ERROR);

		// for custom

		JavaDStream<String> r = streamingContext.receiverStream(new JavaCustomReceiver(HOST, PORT));

		r.print();

		// Receive streaming data from the source
		// JavaReceiverInputDStream<String> lines =
		// streamingContext.socketTextStream(HOST, PORT);
		// lines.print();

		// Execute the Spark workflow defined above
		streamingContext.start();

		streamingContext.awaitTermination();
	}
}