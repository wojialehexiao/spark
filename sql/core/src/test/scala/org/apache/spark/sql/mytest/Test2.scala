package org.apache.spark.sql.mytest

import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.junit.Test

class Test2 {

  @Test
  def test(): Unit ={
    val r = new SparkSqlParser(new SQLConf).parsePlan("SELECT name FROM user")
    println("dd")
  }

}
