package com.dhyan.producer

import java.io.Serializable
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions.asScalaBuffer
import scala.util.control.Breaks.break

import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }

import org.apache.kafka.common.serialization.StringSerializer

import com.dhyan.io.impl.IOServiceImpl

object KafkaProducerService extends Serializable {

  val kafkaParams = Map[String, Object](
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
    ProducerConfig.CLIENT_ID_CONFIG -> "latest"
  //ProducerConfig.ACKS_CONFIG -> "all"
  )

  def extractOptions(properties: Map[String, Any]): Properties = {
    val props = new Properties()
    properties.foreach { case (key, value) => props.put(key, value.toString) }
    props
  }

  def getProducer(properties: Map[String, Any]): KafkaProducer[String, String] = {
    new KafkaProducer[String, String](extractOptions(properties))
  }

  def close(kafkaProducer: KafkaProducer[String, String]): Unit = kafkaProducer.close()

  def produce(kafkaProducer: KafkaProducer[String, String], topic: String, data: String): Unit = {
    val record = new ProducerRecord(topic, "", data)
    kafkaProducer.send(record)
  }
  
  def produceRecords() = {
    println("producer initiated..")
    var counter: Int = 50;
    var topic = "socialConn21"

    var kafkaProducer = getProducer(kafkaParams)

    try {
      while (true) {
        val path = "file:///D:/code/social_circle.txt";
        val records = IOServiceImpl.readRecords(path, "csv", null, null).collectAsList() 

        println("---- starting producer ------")
        records.foreach(event => {
          println("event: "+ event.toString())
          produce(kafkaProducer, topic, event.toString())
          TimeUnit.SECONDS.sleep(20)
        })

        if (counter <= 0) break
        counter -= 1

        TimeUnit.SECONDS.sleep(5)
      }

    } catch {
      case ex: Exception => {
        println("Exception" + ex.getLocalizedMessage)
      }
    } finally {
      kafkaProducer.close();
    }

    println("producer done..")
  }

}  