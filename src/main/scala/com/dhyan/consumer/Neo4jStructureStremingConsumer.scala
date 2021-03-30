package com.dhyan.consumer

import java.io.Serializable

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.from_json

import com.dhyan.common.SparkUtility
import com.dhyan.consumer.writer.neo4jForeachWriter
import com.dhyan.model.User


object neo4jStructureStremingConsumer extends Serializable  {
 
  val sparkSession = SparkUtility.session()
  import sparkSession.implicits._
  
  val neo4jWriter : neo4jForeachWriter = new neo4jForeachWriter("bolt://localhost:7687", "neo4j", "neo4j")
  

  def consumeSocialConnectionRecords1() {
    val df = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "socialConn1")
      //.option("subscribePattern", "topic.*")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()

      df.show()

    //df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    
   /* df.selectExpr("CAST(value AS STRING) AS jsonData")
      .select(from_json($"jsonData", Encoders.product[User].schema).as("data"))
      .select("data.*")
      .writeStream
      .foreachBatch(function)*/
      
  }
  
  
  def consumeSocialConnectionRecords2() {
    val df = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "socialConn1")
      //.option("subscribePattern", "topic.*")
      .option("startingOffsets", "earliest")
      //.option("endingOffsets", "latest")
      .load()

    //df.show()

    //df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    
    val stream = df.selectExpr("CAST(value AS STRING) AS jsonData")
      .select(from_json($"jsonData", Encoders.product[User].schema).as("data"))
      .select("data.*")
      .writeStream
      .foreach(neo4jWriter)
      .start()
      
    stream.awaitTermination
      
  }
  
  
  
  
}