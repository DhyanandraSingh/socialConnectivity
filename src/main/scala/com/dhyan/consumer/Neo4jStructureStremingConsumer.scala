package com.dhyan.consumer

import com.dhyan.common.SparkUtility
import com.dhyan.model.User

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.{from_json, split, when}
import com.dhyan.consumer.writer.neo4jForeachWriter

object neo4jStructureStremingConsumer {

  val sparkSession = SparkUtility.session()
  import sparkSession.implicits._
  
  val neo4jWriter : neo4jForeachWriter = new neo4jForeachWriter("bolt://localhost:7687", "neo4j", "neo4j")
  

  def consumeSocialConnectionRecords1() {
    val df = sparkSession.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092  ")
      .option("subscribe", "socialConn1")
      //.option("subscribePattern", "topic.*")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()

    df.show()

    //df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    
    df.selectExpr("CAST(value AS STRING) AS jsonData")
      .select(from_json($"jsonData", Encoders.product[User].schema).as("data"))
      .select("data.*")
      
  }
  
  
  def consumeSocialConnectionRecords2() {
    val df = sparkSession.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092  ")
      .option("subscribe", "socialConn1")
      //.option("subscribePattern", "topic.*")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()

    df.show()

    //df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    
    df.selectExpr("CAST(value AS STRING) AS jsonData")
      .select(from_json($"jsonData", Encoders.product[User].schema).as("data"))
      .select("data.*")
      .writeStream
      .foreach(neo4jWriter)
      .start()
      
  }
  
  
  
  
}