package com.dhyan.consumer.writer


import java.io.Serializable

import org.apache.spark.sql.ForeachWriter
import org.neo4j.driver.Result

import com.dhyan.model.User
import com.dhyan.utility.db.neo4j.neo4jUtils

class Neo4jWriterV2 extends ForeachWriter[String] with Serializable {

  def insertRecord(user: User): Int = {
    //val session = neo4jdriver.session
    val session = neo4jUtils.initNeo4jDriver()
    val script = s"CREATE (user:Users {name:'${user.id}',last_name:'${user.name}',age:${user.age},city:'${user.email}'})"
    val result: Result = session.run(script)
    session.close()
    result.consume().counters().nodesCreated()
  }

  @Override
  def open(partitionId: Long, version: Long): Boolean = {
    // Open connection
    true
  }

  @Override
  def process(record: String) = {
    // Write string to connection
    
    if (record != null ) {
      
      println("record is not null")
      println("record :" + record.toString())
      var recordArr = record.split("\\|")
      val user = User(
        java.util.UUID.randomUUID.toString,
        recordArr(0),
        recordArr(1).toInt,
        recordArr(2),
        recordArr(3).split(",").toList,
        recordArr(4),
        recordArr(5))

      this.insertRecord(user)
    }
    else
       println("record is null")
  }

  @Override
  def close(errorOrNull: Throwable): Unit = {
    // Close the connection

    //neo4jdriver.close()
  }

}