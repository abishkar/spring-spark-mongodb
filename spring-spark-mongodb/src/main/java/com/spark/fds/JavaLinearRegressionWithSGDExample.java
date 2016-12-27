package com.spark.fds;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

// $example on$
import scala.Tuple2;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
// $example off$

/**
 * Example for LinearRegressionWithSGD.
 */
public class JavaLinearRegressionWithSGDExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("JavaLinearRegressionWithSGDExample").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // $example on$
    // Load and parse the dataå
    String path = "/Users/neparica/Downloads/Zip Files/ExampleData/testGermanCredit.data";
    JavaRDD<String> data = sc.textFile(path);
    JavaRDD<LabeledPoint> parsedData = data.map(
      new Function<String, LabeledPoint>() {
        public LabeledPoint call(String line) {
          String[] parts = line.split(",");
          String[] features = parts[1].split("   ");
          double[] v = new double[features.length];
          for (int i = 0; i < features.length - 1; i++) {
            v[i] = Double.parseDouble(features[i]);
          }
          return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(v));
        }
      }
    );
    
    //[1.568321167117759E-7, 1.1637292217373682E-7, 1.046372573018227E-7, 4.958451954100749E-8, 1.3308641877127057E-7, 9.703417276501022E-8, 6.0940102980205E-8, 0.0]
    parsedData.cache();

    // Building the model
    int numIterations = 100;
    double stepSize = 0.0001;
    final LinearRegressionModel model =
      LinearRegressionWithSGD.train(JavaRDD.toRDD(parsedData), numIterations, stepSize);
    // Evaluate model on training examples and compute training error
    //[1.568321167117759E-7,1.1637292217373682E-7,1.046372573018227E-7,4.958451954100749E-8,1.3308641877127057E-7,9.703417276501022E-8,6.0940102980205E-8,0.0]
    
    JavaRDD<Tuple2<Double, Double>> valuesAndPreds = parsedData.map(
      new Function<LabeledPoint, Tuple2<Double, Double>>() {
        public Tuple2<Double, Double> call(LabeledPoint point) {
          double prediction = model.predict(point.features());
          System.out.println("prediction" + prediction);
          return new Tuple2<>(prediction, point.label());
         
        }
      }
    );
    double MSE = new JavaDoubleRDD(valuesAndPreds.map(
      new Function<Tuple2<Double, Double>, Object>() {
        public Object call(Tuple2<Double, Double> pair) {
          return Math.pow(pair._1() - pair._2(), 2.0);
        }
      }
    ).rdd()).mean();
    System.out.println("training Mean Squared Error = " + MSE);

    // Save and load model
    //model.save(sc.sc(), "target/tmp/javaLinearRegressionWithSGDModel");
    LinearRegressionModel sameModel = LinearRegressionModel.load(sc.sc(),
      "target/tmp/test");
    // $example off$

    sc.stop();
  }
}