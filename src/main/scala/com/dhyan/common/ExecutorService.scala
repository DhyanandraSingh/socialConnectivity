package com.dhyan.common

import org.apache.spark.sql.{ SaveMode, SparkSession, DataFrame, Dataset, Row }
import java.io.Serializable

import com.dhyan.utility.db.neo4j.neo4jUtils
import com.dhyan.common.SparkUtility
import com.dhyan.producer.KafkaProducerService
import com.dhyan.consumer.neo4jStructureStremingConsumer

object ExecutorService extends Serializable {

  def exe1(): Unit = {
    val sparkSession = SparkUtility.session()

    val neo4jDriver = "org.neo4j.spark.DataSource"
    val neo4jUrl = "bolt://localhost:7687"
    val neo4jUsername = "neo4j"
    val neo4jPassword = "neo4j"

    // Write data to neo4j
    import sparkSession.implicits._

    val df = Seq(
      ("Dhyanandra Singh"),
      ("Kumar Rohit")).toDF("name")

    neo4jUtils.writeData(df, SaveMode.ErrorIfExists, neo4jDriver, neo4jUrl, neo4jUsername, neo4jPassword, ":Person")

    // Read data from neo4j using labels
    val neo4jLables = "Person"
    val labelResultDf: DataFrame = neo4jUtils.readDataByLables(sparkSession, neo4jDriver, neo4jUrl, neo4jUsername, neo4jPassword, neo4jLables)
    labelResultDf.show()

    // Read data from neo4j using query
    val query = "MATCH (n:Person) WITH n LIMIT 2 RETURN id(n) as id, n.name as name"
    val quesryResultDf: DataFrame = neo4jUtils.readDataByQuery(sparkSession, neo4jDriver, neo4jUrl, neo4jUsername, neo4jPassword, query)
    quesryResultDf.show()
  }

  def producerExe(): Unit = {
    println("producer called.....")
    KafkaProducerService.produceRecords();
  }

  def forEachConsumerExe(): Unit = {
    println("foreach consumer called.....");
    neo4jStructureStremingConsumer.consumeSocialConnectionRecords2();
  }

  def forEachBatchConsumerExe(): Unit = {
    println("foreach batch consumer called.....");
    neo4jStructureStremingConsumer.consumeSocialConnectionRecords1();
  }

  def main(args: Array[String]) {
    
    print("-------------------------"+args(0)+"--------------------------")

    if (args != null && args.length != 0 && args(0).equalsIgnoreCase("producer")) {
      producerExe()
    } else if (args != null && args.length != 0 && args(0).equalsIgnoreCase("foreachConsumer")) {
      forEachConsumerExe()
    } else if (args != null && args.length != 0 && args(0).equalsIgnoreCase("foreachBatchConsumer")) {
      forEachBatchConsumerExe();
    } else
      exe1()

  }

} 