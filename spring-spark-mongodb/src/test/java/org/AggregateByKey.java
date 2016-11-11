package org;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class AggregateByKey {
	public static void main(String[] args) {
		aggregateByKey();
		
	}
	
	
	
	     
	     public static void aggregateByKey() {
		JavaSparkContext sc = new JavaSparkContext("local", "JavaAPISuite");
		  List l=Arrays.asList(
		           new Tuple2<>(1, 2),
		           new Tuple2<>(1, 1),
		           new Tuple2<>(3, 2),
		           new Tuple2<>(5, 1),
		           new Tuple2<>(5, 3),
		           new Tuple2<>(1, 1)
		           
				  );
	       JavaPairRDD<Integer, Integer> pairs = sc.parallelizePairs(
	       l, 1000);
	   
	       Map<Integer, Set<Integer>> sets = pairs.aggregateByKey(new HashSet<Integer>(),
	         new Function2<Set<Integer>, Integer, Set<Integer>>() {
	           @Override
	           public Set<Integer> call(Set<Integer> a, Integer b) {
	             a.add(b);
	             return a;
	           }
	         },
	         new Function2<Set<Integer>, Set<Integer>, Set<Integer>>() {
	           @Override
	           public Set<Integer> call(Set<Integer> a, Set<Integer> b) {
	             a.addAll(b);
	             return a;
	           }
	         }).collectAsMap();
	       
	       System.out.println(sets);
//	     
	     }

}
