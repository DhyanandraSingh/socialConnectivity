package com.dhyan.generators

import java.time.LocalDateTime
import com.dhyan.model.Relationship
import com.dhyan.model.User

import java.io.Serializable

trait DataGen extends Serializable {
  
  def userGen(totalUser: Integer, startDate:LocalDateTime, endDate: LocalDateTime) : List[User]
  
  def relationshipGen(followerId: String, FollowedId: String, startDate:LocalDateTime, endDate: LocalDateTime) : Relationship
  
  
}