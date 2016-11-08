package org;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.junit.Test;



public class AggregateTest {
	
public static void main(String[] args) {
	aggregate();
	
}
	    public static  void aggregate() {
		JavaSparkContext sc = new JavaSparkContext("local", "JavaAPISuite");
	    JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));
	     int sum = rdd.aggregate(0, new AddInts(), new AddInts());
	   System.out.println(sum);
	  }
	
  
	
	 private static class AddInts implements Function2<Integer, Integer, Integer> {
		       public Integer call(Integer a, Integer b) {
		         return a + b;
		       }
		     }
		   
	
	
}
