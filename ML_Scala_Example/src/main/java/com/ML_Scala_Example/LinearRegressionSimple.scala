package com.ML_Scala_Example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.ml.Pipeline
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LinearRegressionWithSGD



object LinearRegressionSimple {
  
  def main(args: Array[String]){
    
    val conf = new SparkConf().setAppName("test")
    val sc=new SparkContext(conf);
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    var dataDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("/user/hadoop/testdata/input.csv").cache()

   dataDF= dataDF.withColumn("label", dataDF.col("length_of_Membership").cast(DataTypes.DoubleType))
    
    val addressIndex = new StringIndexer().setInputCol("Address").setOutputCol("AddressIndex")
    val encoder = new OneHotEncoder().setInputCol("AddressIndex").setOutputCol("addressvect")
    
    val vecrorArray = Array("addressvect", "Avg_Session_length", "time_on_app", "time_on_website", "length_of_Membership")
    val assembler = new VectorAssembler().setInputCols(vecrorArray).setOutputCol("features")
     val lr = new LinearRegression();
    lr.setFeaturesCol("features").setPredictionCol("prediction")
   
    val pipeline = new Pipeline().setStages(Array(addressIndex,encoder,assembler,lr))
    
    val splits = dataDF.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()
    val test = splits(1).cache()
    
    
    val model = pipeline.fit(training)
    val predictionResult = model.transform(test).select("label", "prediction")
    
     val redeictioAndLabels = predictionResult.map {
        row => (row.get(0).asInstanceOf[Double], row.get(1).asInstanceOf[Double])
      }
    val redeictioAndLabelsrdd : RDD[(Double, Double)] = redeictioAndLabels.rdd
    val MSE = redeictioAndLabelsrdd.map{ case(v, p) => math.pow((v - p), 2) }.mean()
    println("training Mean Squared Error = " + MSE)
    val rmse   = math.sqrt(MSE)
    println(s"Root-mean-square error = $rmse")
    
    //other way to find root mean square error
   
   val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("label").setPredictionCol("prediction")
  
  val rmse1 = evaluator.evaluate(predictionResult)
  println(s"Root-mean-square error = $rmse1")
  
  //save model for prediction
  model.write.overwrite().save("/user/hadoop/testdata/webprediction/")
  
  
    
  }
}