package com.ML_Scala_Example.kafka

import java.util.Properties
import java.util.UUID

import org.apache.avro.io._
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import scala.io.Source
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericData
import org.apache.avro.specific.SpecificDatumWriter
import java.io.ByteArrayOutputStream

object KafkaAvroProducerExample{

case class WebData(Email: String, Address: String, Avg_Session_length:Double,time_on_app:Double,time_on_website:Double,length_of_Membership:Int)

def main(args: Array[String]){

	val props = new Properties()
			props.put("metadata.broker.list", "localhost:9092")
			props.put("message.send.max.retries", "5")
			props.put("request.required.acks", "-1")
			props.put("serializer.class", "kafka.serializer.DefaultEncoder")
			props.put("client.id", UUID.randomUUID().toString())

			val producer = new Producer[String, Array[Byte]](new ProducerConfig(props))

			//Read avro schema file and
			// val schema: Schema = new Parser().parse(Source.fromURL(getClass.getResource("/schema.avsc")).mkString)
			val schema: Schema = new Parser().parse(Source.fromFile("/home/hadoop/testdata/web.avsc").mkString)


			def send(topic:String, users:List[WebData]) : Unit ={
					val genericUser: GenericRecord = new GenericData.Record(schema)

							try{
								val messages= users.map { user =>
								genericUser.put("Email", user.Email)
								genericUser.put("Address", user.Address)
								genericUser.put("Avg_Session_length", user.Avg_Session_length)
								genericUser.put("time_on_app", user.time_on_app)
								genericUser.put("time_on_website", user.time_on_website)
								genericUser.put("length_of_Membership", user.length_of_Membership)

								// Serialize generic record object into byte array
								val writer = new SpecificDatumWriter[GenericRecord](schema)
								val out = new ByteArrayOutputStream()
								val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)

								writer.write(genericUser, encoder)
								encoder.flush()
								out.close()

								val serializedBytes: Array[Byte] = out.toByteArray()

								new KeyedMessage[String, Array[Byte]](topic, serializedBytes)


								}
								producer.send(messages: _*)
							}catch{
							case ex: Exception => ex.printStackTrace()
							}

			}


			val topic = "avro-topic"
					val w1 = WebData("a@a.com","USA",35.5,15.0,38.0,120)
					val w2 = WebData("b@a.com","INDIA",15.5,10.0,38.0,90)

					send(topic, List(w1, w2))

}
}