package com.dhyan.generators.impl

import scala.collection._

import java.time.LocalDateTime
import java.util.concurrent.ThreadLocalRandom
import java.util.Random

import java.io.Serializable

import com.dhyan.generators.DataGen

import com.dhyan.model.Relationship
import com.dhyan.model.User

class DataGenImpl extends Serializable with DataGen {
  
  def userGen(totalUser: Integer, startDate:LocalDateTime, endDate: LocalDateTime): List[User] = {
    var users = List[User] ()
    for (i <- 1 to totalUser){
      var usr = User((1000+1).toString(),"User"+String.valueOf(i), new Random().nextInt(50), "user"+String.valueOf(i)+"@fractal.ai", List("English", "Hindi"), "India", LocalDateTime.now().toString())
      usr :: users
    }
    users
  }
  
  def relationshipGen(followerId: String, FollowedId: String, startDate:LocalDateTime, endDate: LocalDateTime): Relationship ={
    //new Relationship()
    null
  }
  
} 