package com.dhyan.common

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object BasicOps {
  
  
  def stringToDatetimeCoverter(dateStr: String, format: String = "yyyy-MM-dd HH:mm:ss"): LocalDateTime = {
    
    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern(format)
    val dateTime: LocalDateTime = LocalDateTime.parse(dateStr, formatter)
    
    dateTime
  }

  
}