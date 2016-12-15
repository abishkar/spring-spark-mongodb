package org.machinelearning.example;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

// $example on$
import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
public class JavaLogisticRegressionWithLBFGSExample {
  public static void main(String[] args) {
	  
	  
    SparkConf conf = new SparkConf().setAppName("JavaLogisticRegressionWithLBFGSExample").setMaster("local");
    SparkContext sc = new SparkContext(conf);
    // $example on$
    String path = "/Users/neparica/Downloads/spark-2.0.1-bin-hadoop2.7 2/data/mllib/sample_libsvm_data.txt";
    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, path).toJavaRDD();

    // Split initial RDD into two... [60% training data, 40% testing data].
    JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] {0.6, 0.4}, 11L);
    JavaRDD<LabeledPoint> training = splits[0].cache();
    JavaRDD<LabeledPoint> test = splits[1];

    // Run training algorithm to build the model.
    final LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(training.rdd());

    // Compute raw scores on the test set.
    JavaRDD<Tuple2<Object, Object>> predictionAndLabels = test.map(
      new Function<LabeledPoint, Tuple2<Object, Object>>() {
        public Tuple2<Object, Object> call(LabeledPoint p) {
          Double prediction = model.predict(p.features());
          return new Tuple2<Object, Object>(prediction, p.label());
        }
      }
    );

    // Get evaluation metrics.
    MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
    double accuracy = metrics.fMeasure();
    System.out.println("Accuracy = " + accuracy);

    // Save and load model
    model.save(sc, "target/tmp/javaLogisticRegressionWithLBFGSModel");
    LogisticRegressionModel sameModel = LogisticRegressionModel.load(sc,
      "target/tmp/javaLogisticRegressionWithLBFGSModel");
    // $example off$

    sc.stop();
  }
}