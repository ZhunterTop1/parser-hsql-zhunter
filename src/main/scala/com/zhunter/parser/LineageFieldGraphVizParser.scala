package com.zhunter.parser

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import LineageFieldGraphVizParser.{edges, structMap}
import com.zhunter.model.LineageTable

import scala.collection.mutable
import scala.collection.mutable._
import java.util.HashMap
import java.util.HashSet
import javafx.util.Pair

object LineageFieldGraphVizParser{
  private var id = -1l
  val structMap =  new HashMap[String, String]()
  val edges = new HashSet[Pair[String, String]]()
  def addId(): Unit ={
    id+=1
  }
  def getId(): Long ={
    id
  }
  def clear(): Unit ={
    structMap.clear()
    edges.clear()
  }
}

class LineageFieldGraphVizParser {
  def resetId(): Unit ={
    LineageFieldGraphVizParser.id = -1l
  }
  def getStructField(): HashMap[String, String] ={
    structMap
  }
  def getEdges(): HashSet[Pair[String, String]] ={
    edges
  }
  def createFieldString(tree: LogicalPlan, fieldName: String) :Unit={
    LineageFieldGraphVizParser.clear()
    val table: LineageTable = new LineageTableParser().createTableGraph(tree)
    val cpFieldName: Set[String] = Set()
    cpFieldName.add(fieldName)
    val modifyTable = createFieldString(table, cpFieldName, LineageFieldGraphVizParser.structMap, LineageFieldGraphVizParser.edges, LineageFieldGraphVizParser.getId())
    println(modifyTable)
  }
  def createFieldString(tree: LogicalPlan, offset: Int) :Unit={
    LineageFieldGraphVizParser.clear()
    val table: LineageTable = new LineageTableParser().createTableGraph(tree)
    val cpFieldName: Set[String] = Set()
    cpFieldName.add(table.getMetaField()(offset).getFieldName())
    val modifyTable = createFieldString(table, cpFieldName, LineageFieldGraphVizParser.structMap, LineageFieldGraphVizParser.edges, LineageFieldGraphVizParser.getId())
    println(modifyTable)
  }
  def createFieldString(table: LineageTable, offset: Int) :Unit={
    LineageFieldGraphVizParser.clear()
    val cpFieldName: Set[String] = Set()
    cpFieldName.add(table.getMetaField()(offset).getFieldName())
    val modifyTable = createFieldString(table, cpFieldName, LineageFieldGraphVizParser.structMap, LineageFieldGraphVizParser.edges, LineageFieldGraphVizParser.getId())
    println(modifyTable)
  }
  def createFieldString(table: LineageTable, fieldName: String) :Unit={
    LineageFieldGraphVizParser.clear()
    val cpFieldName: Set[String] = Set()
    cpFieldName.add(fieldName)
    val modifyTable = createFieldString(table, cpFieldName, LineageFieldGraphVizParser.structMap, LineageFieldGraphVizParser.edges, LineageFieldGraphVizParser.getId())
    println(modifyTable)
  }
  def createFieldString(table: LineageTable, fieldNameSet: Set[String], structMap: HashMap[String, String], edges: HashSet[Pair[String, String]], fatherId: Long): LineageTable ={
    LineageFieldGraphVizParser.addId()
    val id = LineageFieldGraphVizParser.getId()
    val cpTable: LineageTable = table.clone()
    val cpFieldName: Set[String] = Set()
    for(field <- table.getMetaField()){
      if(fieldNameSet.contains(field.getFieldName())){
        field.getReference().foreach(cpFieldName.add(_))
        cpTable.addMetaField(field)
        val fieldName = field.getFieldName()
        if(fatherId != -1){
          edges.add(new Pair[String, String](s"struct$fatherId:$fieldName", s"struct$id:f#$fieldName"))
        }
      }
    }
    for(child <- table.getChildrenTable()){
      cpTable.addChildrenTable(createFieldString(child, cpFieldName, structMap, edges, id))
    }
    val fieldStr: mutable.StringBuilder = new StringBuilder()
    val refers: mutable.Set[String] = Set[String]()
    for(field <- cpTable.getMetaField()){
      if(!fieldStr.isEmpty) fieldStr.append("|")
      fieldStr.append(s"{{<f#${field.fieldName}> ${field.getFieldName()}}|{<fe#${field.fieldName}> ${field.getExpression()}}}")
      for(refer <- field.getReference()){
        refers.add(refer)
      }
    }
    val referStr = new StringBuilder()
    for(refer <- refers){
      if(!referStr.isEmpty) referStr.append("|")
      referStr.append(s"<$refer> $refer")
    }
//    $fatherId:$id
    val structName = s"{{$fieldStr}|{<f0> tableName: ${cpTable.tableName}}|{$referStr}}"
//    println(structId, structName)
    if(cpTable.getMetaField().size != 0) {
      structMap.put(s"struct$id", structName)
    }
    cpTable
  }
}











