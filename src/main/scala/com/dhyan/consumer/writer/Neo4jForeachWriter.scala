package com.dhyan.consumer.writer

import org.apache.spark.sql.{ Row, ForeachWriter }

//import org.neo4j.driver.v1._
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Result
import org.neo4j.driver.Session

import scala.collection.JavaConversions._
import java.util.ArrayList
import java.util.List


import com.dhyan.model.User
import com.dhyan.common.BasicOps

class neo4jForeachWriter(uri: String, user: String, password: String) extends ForeachWriter[Row] {

  //var neo4jdriver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))

  var neo4jdriver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "neo4j"))

  def insertRecord(user: User): Int = {
    val session = neo4jdriver.session
    val script = s"CREATE (user:Users {name:'${user.id}',last_name:'${user.name}',age:${user.age},city:'${user.email}'})"
    /*val result: StatementResult = session.run(script)
    session.close()
    result.consume().counters().nodesCreated()*/
    val result: Result = session.run(script)
    session.close()
    result.consume().counters().nodesCreated()
    

    /*try {
      for (session <- managed(neo4jdriver.session()))
        session.readTransaction((tx) => {
          val names: List[String] = new ArrayList[String]()
          val result: Result =
            tx.run("MATCH (a:Person) RETURN a.name ORDER BY a.name")
          while (result.hasNext) names.add(result.next().get(0).asString())
          names
        })
    }*/

  }

  @Override
  def open(partitionId: Long, version: Long): Boolean = {
    // Open connection
    true
  }

  @Override
  def process(record: Row) = {
    // Write string to connection

    val user = User(
      java.util.UUID.randomUUID.toString,
      record.getString(0),
      record.getInt(1),
      record.getString(2),
      record.getString(3).split(",").toList,
      record.getString(4),
      /*BasicOps.stringToDatetimeCoverter(record.getString(5), "yyyy-MM-dd HH:mm:ss")*/
      record.getString(5))

    this.insertRecord(user)
  }

  @Override
  def close(errorOrNull: Throwable): Unit = {
    // Close the connection

    neo4jdriver.close()
  }

}