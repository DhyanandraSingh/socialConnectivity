package com.dhyan.utility.db.neo4j

import org.apache.spark.sql.{ SaveMode, SparkSession }
import org.apache.spark.sql.{ DataFrame, Dataset, Row }
import org.apache.spark.sql.types.{ StringType, StructField, StructType }
import java.io.Serializable

object neo4jUtils extends Serializable {

  def writeData(df: DataFrame, saveMode: SaveMode, driver: String, url: String, username: String, password: String, labels: String) = {

    df.write.format(driver)
      .mode(saveMode)
      .option("url", url)
      .option("authentication.basic.username", username)
      .option("authentication.basic.password", password)
      .option("labels", labels)
      .save()
  }

  def readDataByLables(sparkSession: SparkSession, driver: String, url: String, username: String, password: String, labels: String): DataFrame = {
    val df = sparkSession.read.format(driver)
      .option("url", url)
      .option("authentication.basic.username", username)
      .option("authentication.basic.password", password)
      .option("labels", labels)
      .load()

    df
  }

  def readDataByQuery(sparkSession: SparkSession, driver: String, url: String, username: String, password: String, query: String): DataFrame = {
    val df = sparkSession.read.format(driver)
      .option("url", url)
      .option("authentication.basic.username", username)
      .option("authentication.basic.password", password)
      .option("query", query)
      .load()

    df
  }
}