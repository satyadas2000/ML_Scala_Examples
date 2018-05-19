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
    
    val model = pipeline.fit(dataDF)
    val predictionResult = model.transform(dataDF)
   
   val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("label").setPredictionCol("prediction")
  
  val rmse = evaluator.evaluate(predictionResult)
println(s"Root-mean-square error = $rmse")
  
    
  }
}