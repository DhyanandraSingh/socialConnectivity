package com.dhyan.io.impl

import com.dhyan.io.IOService
import com.dhyan.common.SparkUtility

import java.io.FileNotFoundException
import org.apache.spark.sql.types.StructType

import org.apache.spark.sql.{ SaveMode, SparkSession, DataFrame, Dataset, Row }


object IOServiceImpl extends IOService {
  
  val sparkSession :SparkSession = SparkUtility.session()
  
   override def readRecords(path: String, format: String, schema: StructType = null, options: Map[String, String] = null): DataFrame = {

    println("\n--------x---------------  reading file  ----------------x---------\n");
    var df: DataFrame = null
    var rowTag: String = null
    try {
      if (format.equalsIgnoreCase("xml")) {
        if (options != null) rowTag = options("rowTag")
        if (schema.isEmpty || schema == null)
          df = sparkSession.read.format("xml").option("rowTag", rowTag).load(path)
        else
          df = sparkSession.read.format("xml").option("rowTag", rowTag).schema(schema).load(path)

      } else if (format.equalsIgnoreCase("csv")) {
        df = sparkSession.read.format("csv").option("header", "true").option("charset", "UTF8").option("sep", ",").load(path)

      } else if (format.equalsIgnoreCase("json")) {
        df = sparkSession.read.format("json").option("multiline", "true").load(path)

      } else {
        df = sparkSession.read.text(path)
      }
    } catch {
      case ex: FileNotFoundException => {
        println(s"File path not found")
      }
      case unknown: Exception => {
        println(s"Unknown exception: $unknown")
      }
    }
    return df
  }
   
   override def write(ds: Dataset[Row], path: String) = {

  }


  
}