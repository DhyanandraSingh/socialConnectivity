package com.dhyan.consumer

import java.io.Serializable

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types._
//import org.apache.spark.sql.functions._

import com.dhyan.common.SparkUtility
import com.dhyan.consumer.writer.Neo4jWriterV2
import com.dhyan.model.User

object neo4jStructureStremingConsumer extends Serializable {

  val sparkSession = SparkUtility.session()
  import sparkSession.implicits._

  //val neo4jWriter: neo4jForeachWriter = new neo4jForeachWriter("bolt://localhost:7687", "neo4j", "neo4j")
  val neo4jWriterV2: Neo4jWriterV2 = new Neo4jWriterV2()

  def consumeSocialConnectionRecords1() {
    val df = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "socialConn21")
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
      .option("subscribe", "socialConn21")
      //.option("subscribePattern", "topic.*")
      .option("startingOffsets", "earliest")
      //.option("endingOffsets", "latest")
      .load()

    //df.show()

    //df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

    val q1 = {
      df
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
        .select("value")
        /*.select(to_json(struct("*")) as 'body)
        .selectExpr("cast(body as string)")*/
        .as[String]
        .writeStream
        .outputMode("update")
        //.format("console")
        .foreach(neo4jWriterV2)
        .start()
    }

    /*val stream = df.selectExpr("CAST(value AS STRING) AS jsonData")
      .select(from_json($"jsonData", Encoders.product[User].schema).as("data"))
      .select("data.*")
      .writeStream
      .foreach(neo4jWriter)
      .start()*/

    q1.awaitTermination
    //stream.awaitTermination
  }

}