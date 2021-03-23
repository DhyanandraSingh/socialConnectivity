package com.dhyan.consumer.writer

import org.apache.spark.sql.{Row, ForeachWriter}

import org.neo4j.driver.v1._

import com.dhyan.model.User
import com.dhyan.common.BasicOps

class neo4jForeachWriter(uri: String, user: String, password: String) extends ForeachWriter[Row] {
  
  var neo4jdriver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))
  
  def insertRecord(user: User): Int = {
    val session = neo4jdriver.session
    val script = s"CREATE (user:Users {name:'${user.id}',last_name:'${user.name}',age:${user.age},city:'${user.email}'})"
    val result: StatementResult = session.run(script)
    session.close()
    result.consume().counters().nodesCreated()
  }

  @Override
  def open(partitionId: Long, version: Long): Boolean = {
    // Open connection
    true
  }

  @Override
  def process(record: Row) = {
    // Write string to connection
          
      val user = User(java.util.UUID.randomUUID.toString, 
          record.getString(0), 
          record.getInt(1), 
          record.getString(2), 
          record.getString(3).split(",").toList, 
          record.getString(4), 
          BasicOps.stringToDatetimeCoverter(record.getString(5), "yyyy-MM-dd HH:mm:ss"))
      
      this.insertRecord(user)
    }
    
  @Override
  def close(errorOrNull: Throwable): Unit = {
    // Close the connection
    
    neo4jdriver.close()
  }

}