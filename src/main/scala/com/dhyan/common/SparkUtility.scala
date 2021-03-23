package com.dhyan.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ Duration, StreamingContext }

object SparkUtility {

  @transient private var instance: SparkSession = _

  def sparkConfig(): SparkConf = {
    return new SparkConf()
      .setMaster("local[*]")
      .setAppName("SocialConnectivity")
  }

  def session(): SparkSession = {
    if (Option(instance).isEmpty) {
      instance = SparkSession.builder().config(sparkConfig()).getOrCreate()
    }
    instance
  }

  def streamingContext(): StreamingContext = {
    return new StreamingContext(sparkConfig(), new Duration(10000))
  }
}