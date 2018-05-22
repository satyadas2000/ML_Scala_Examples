package com.ML_Scala_Example.kafka

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.SparkSession
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericData
import org.apache.avro.specific.SpecificDatumWriter

import scala.reflect.runtime.universe._
import java.sql.Timestamp
import scala.io.Source
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.DecoderFactory
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.types.IntegerType

object SparkLRpredictionAvroMessages {
  

  case class WebData(Email: String, Address: String, Avg_Session_length:Double,time_on_app:Double,time_on_website:Double,length_of_Membership:Int)
val schema = StructType(List(
		StructField("Email", StringType, true),
		StructField("Address", StringType, true),
		StructField("Avg_Session_length", DoubleType, true),
		StructField("time_on_app", DoubleType, true),
		StructField("time_on_website", DoubleType, true),
		StructField("length_of_Membership", IntegerType, true)
		)
)
  
  val messageSchema = new Schema.Parser().parse(Source.fromFile("/home/hadoop/testdata/web.avsc").mkString)
val reader = new GenericDatumReader[GenericRecord](messageSchema)
// Binary decoder
val decoder = DecoderFactory.get()

def main(args: Array[String]){
	val KafkaBroker = "localhost:9092";
	val InTopic = "avro-topic";

	// Get Spark session
	val session = SparkSession
			.builder
			.master("local[*]")
			.appName("myapp")
			.getOrCreate()

			// Load streaming data
			import session.implicits._
			
			
			val model = PipelineModel.load("/user/hadoop/testdata/webprediction/")

			def processRow(row: Row) = {
		val spark = SparkSession
				.builder
				.master("local[*]")
				.appName("myapp")
				.getOrCreate()

				val rdd = spark.sparkContext.makeRDD(List(row))
				val dataFrame = spark.createDataFrame(rdd, schema)

				dataFrame.show()

				val withoutLabelTest = model.transform(dataFrame)

				val lpTest1 = withoutLabelTest.select( "prediction")
				lpTest1.show()

	}

			
			
			
			
			val data = session
			.readStream
			.format("kafka")
			.option("kafka.bootstrap.servers", KafkaBroker)
			.option("subscribe", InTopic)
			.load()
			.select($"value".as[Array[Byte]])
			.map(d => {
				val rec = reader.read(null, decoder.binaryDecoder(d, null))
						val Email = rec.get("Email").toString
						val Address = rec.get("Address").toString
						val Avg_Session_length = rec.get("Avg_Session_length").toString.toDouble
						val time_on_app = rec.get("time_on_app").toString.toDouble
						val time_on_website = rec.get("time_on_website").toString.toDouble
						val length_of_Membership = rec.get("length_of_Membership").toString.toInt

						// val name = rec.get("name").asInstanceOf[Byte].toString
						//  val email = rec.get("email").asInstanceOf[Byte].toString

						new WebData(Email, Address, Avg_Session_length,time_on_app,time_on_website,length_of_Membership)
			})


			//save tp perquet

			val query = data.writeStream.outputMode("Append").format("parquet")        // can be "orc", "json", "csv", etc.
			.option("path", "/user/hadoop/testdata/parquet/webdata/").option("checkpointLocation", "checkpoint")
			.start()

		val writer = new ForeachWriter[WebData] {
		override def open(partitionId: Long, version: Long) = true
				override def process(value: WebData) = {
			println("Email"+value.Email+"*****************************************************")
			val dataseq = List(value.Email,value.Address,value.Avg_Session_length,value.time_on_app,value.time_on_website,value.length_of_Membership)
			val row = Row.fromSeq(dataseq)
			processRow(row)

		}
		override def close(errorOrNull: Throwable) = {}
	}

val query1 = data.writeStream.foreach(writer).start()

			query.awaitTermination()
  }
}