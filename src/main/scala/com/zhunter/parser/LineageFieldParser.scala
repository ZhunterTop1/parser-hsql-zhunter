package com.zhunter.parser

import com.zhunter.model.LineageTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import scala.collection.mutable._

class LineageFieldParser {
  def createFieldGraph(tree: LogicalPlan, fieldName: String): LineageTable ={
    val table: LineageTable = new LineageTableParser().createTableGraph(tree)
    val cpFieldName: Set[String] = Set()
    cpFieldName.add(fieldName)
    createFieldGraph(table, cpFieldName)
  }
  def createFieldGraph(table: LineageTable, fieldNameSet: Set[String]): LineageTable ={
    val cpTable: LineageTable = table.clone()
    val cpFieldName: Set[String] = Set()
    for(field <- table.getMetaField()){
      if(fieldNameSet.contains(field.getFieldName())){
        field.getReference().foreach(cpFieldName.add(_))
        cpTable.addMetaField(field)
      }
    }
    for(child <- table.getChildrenTable()){
      cpTable.addChildrenTable(createFieldGraph(child, cpFieldName))
    }
    cpTable
  }
}
