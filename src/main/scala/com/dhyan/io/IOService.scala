package com.dhyan.io

import org.apache.spark.sql.{ DataFrame, Dataset, Row }
import org.apache.spark.sql.types.StructType


trait IOService {
  
      def readRecords(path: String, format: String, schema: StructType, options: Map[String, String] = null): DataFrame
	
      def write(ds: Dataset[Row], path: String)
  
}