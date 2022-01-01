package com.zhunter.driver

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SessionState

object SparkSessionDriver {
  var spark: SparkSession = null
  def init(sparkSession: SparkSession = null): Unit ={
    if(spark == null){
      if(sparkSession == null){
        val conf = new SparkConf()
          .setAppName("q")
          .setMaster("local[*]")
          .set("spark.executor.memory", "500m")
          .set("spark.cores.max", "1")
        spark = SparkSession.builder().config(conf).getOrCreate()
      }else{
        spark = sparkSession
      }
    }
  }

  def acceptLogicalPlan(sql: String): LogicalPlan ={
    if(spark == null) init()
    val parser = spark.sessionState.sqlParser
    val plan: LogicalPlan = parser.parsePlan(sql)
    val analyzed: LogicalPlan = spark.sessionState.analyzer.execute(plan)
    analyzed
  }
}
class SparkSessionDriver{}
