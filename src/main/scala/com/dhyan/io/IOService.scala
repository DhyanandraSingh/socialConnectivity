package com.dhyan.io

import org.apache.spark.sql.{ DataFrame, Dataset, Row }
import org.apache.spark.sql.types.StructType

import java.io.Serializable

trait IOService extends Serializable {
  
      def readRecords(path: String, format: String, schema: StructType, options: Map[String, String] = null): DataFrame
	
      def write(ds: Dataset[Row], path: String)
  
}