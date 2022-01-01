package com.zhunter.parser

import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan}
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable

import scala.util.control.Breaks
/**
 * 'InsertIntoStatement 'UnresolvedRelation [AAA], [], false, Map(date -> Some(xx)), true, false
*+- Aggregate [cnt#0], [sum(cast(cnt#0 as bigint)) AS cnt#4L, 7 AS quit#5]
   *+- SubqueryAlias a
      *+- Union false, false
         *:- Project [1 AS cnt#0, 7 AS qqq#1]
         *:  +- OneRowRelation
         *+- Project [2 AS ppp#2, 4 AS xxx#3]
            *+- OneRowRelation
 * */
class InsertTableParser {
  def parserInsertTable(tree: LogicalPlan, tableName: String=null, fieldName: String): Int ={
    var offset = -1
    tree match {
      case statement: InsertIntoStatement =>{
        val table: LogicalPlan = statement.table
        val cols: Seq[String] = statement.userSpecifiedCols
        val loop = new Breaks;
        loop.breakable(
          for(i <- 0 until cols.size){
            if(cols(i).split("\\.")(cols(i).split("\\.").size-1).equals(fieldName.equals(fieldName))){
              offset = i
              loop.break
            }
          }
        )
      }
      case hiveTable: InsertIntoHiveTable =>{
        val hiveTableName = hiveTable.table.identifier.database.head + "." + hiveTable.table.identifier.table
        val columns = hiveTable.outputColumnNames
        if(tableName != null && !tableName.equals(hiveTableName)){
          throw new Exception("表名不正确, 请再次调试解析工具")
        }
        val loop = new Breaks;
        loop.breakable(
          for(i <- 0 until columns.size){
            if(columns(i).equals(fieldName.equals(fieldName))){
              offset = i
              loop.break
            }
          }
        )
      }
      case _ => {}
    }
    offset
  }
}
