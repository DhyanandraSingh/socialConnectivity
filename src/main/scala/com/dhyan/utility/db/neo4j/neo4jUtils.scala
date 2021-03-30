package com.dhyan.utility.db.neo4j

import org.apache.spark.sql.{ SaveMode, SparkSession }
import org.apache.spark.sql.{ DataFrame, Dataset, Row }
import org.apache.spark.sql.types.{ StringType, StructField, StructType }

import java.io.Serializable

import org.neo4j.driver.{ Driver, GraphDatabase, AuthTokens, Result, Session }

object neo4jUtils extends Serializable {

  def initNeo4jDriver(): Session = {

    //var neo4jdriver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))

    val neo4jdriver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "neo4j"))
    val session = neo4jdriver.session

    session

  }

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