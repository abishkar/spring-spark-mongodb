package org.machinelearning.example;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.mllib.regression.LabeledPoint;
public class MachineLearning {
	
	public static void main(String[] args) {
		

		// Create a dense vector (1.0, 0.0, 3.0).
		Vector dv = Vectors.dense(1.0, 0.0, 3.0);
		// Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
		Vector sv = Vectors.sparse(3, new int[] {0, 2}, new double[] {1.0, 3.0});
		
		// Create a labeled point with a positive label and a dense feature vector.
		LabeledPoint pos = new LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0));
		
		
		String logFile = "/Users/bigdinosaur/Desktop/datarepo/part-00000"; // Should be some file on your system
	    //SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory", "2g");
	 SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("spark://localhost:7077").set("spark.driver.cores", "1");
	 
	  // SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("spark://linus:7077");

	 
	 

	    
	    
	    JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		
		
	    		double[][] array = {{1.12, 2.05, 3.12}, {5.56, 6.28, 8.94}, {10.2, 8.0, 20.5}};
	    LinkedList<Vector> rowsList = new LinkedList<>();
	    List<LabeledPoint> list = new ArrayList<LabeledPoint>();
	    for (int i = 0; i < array.length; i++) {
	      Vector currentRow = Vectors.dense(1,array[i]);
	      rowsList.add(currentRow);
	      LabeledPoint neg = new LabeledPoint(i, currentRow);;
	      
	      list.add(neg);
	      
	      
	    }
	    JavaRDD<Vector> rows = sc.parallelize(rowsList);
	    JavaRDD<LabeledPoint> data = sc.parallelize(list);

	    // Create a RowMatrix from JavaRDD<Vector>.
	RowMatrix mat = new RowMatrix(rows.rdd());
	long m = mat.numRows();
	long n = mat.numCols();

	
	 org.apache.spark.mllib.classification.LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
            .setNumClasses(2)
            .run(data.rdd());
	
	
	
	
	 model.save(sc.sc(), "LRParquet");
		
		
		
		// Create a labeled point with a negative label and a sparse feature vector.
	//	LabeledPoint neg = new LabeledPoint("duck", Vectors.sparse(3, new String[] {"walks", "quacks"}, new String[] {"swim","play"}));
		
		
		//walks, swims, and quacks like a duck, then the label is "duck". 
		//features =if question and properties 
	//	if(walk) && if (swim) && if(quacks)
		
		
		//LabeledPoint neg = new LabeledPoint("fraudamount", Vectors.sparse(3, new String[] {"walks", "quacks"}, new String[] {"swim","play"}));
	
		
//		for example bhatbhatini super market best product is xyz ,so xyz=label
//				its features are if it is sold all over nepal & it has high profit on sale & it is the most sold product in all brances 
	}

}
