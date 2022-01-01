package com.zhunter

import MainZhunter.getParserStruct
import com.zhunter.driver.SparkSessionDriver
import com.zhunter.graph.GraphViz
import com.zhunter.model.LineageTable
import com.zhunter.parser.{InsertTableParser, LineageFieldGraphVizParser, LineageTableParser}
import javafx.util.Pair
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import java.io.File
import java.util.{HashMap, HashSet}
class MainZhunter{
  var id: Long = -1l
  def getId(): Long ={
    id+=1
    this.id
  }
  def createImg(session: SparkSession, sql: String, fieldName: String, tableName: String): Unit ={
    val split: Array[String] = sql.split("\\s*;\\s*(?=([^']*'[^']*')*[^']*$)")
    var useSql = sql
    for (str <- split) {
      if (str != null && str.contains(tableName.trim)) {
        useSql = str
      }
    }
    SparkSessionDriver.init(session)
    val plan: LogicalPlan = SparkSessionDriver.acceptLogicalPlan(useSql)
    val parser: LineageFieldGraphVizParser = getParserStruct(plan, fieldName, tableName)
    val fields: HashMap[String, String] = parser.getStructField()
    val edges: HashSet[Pair[String, String]] = parser.getEdges()
    val dotFormat = "pdf"
    val fileName = f"fullTimeTest${getId()}"
    val gv = new GraphViz
    gv.addln(gv.start_graph)
    gv.addln("\tnode [shape=record]")
    //struct1 [label="{{{{<f#cnt#0> cnt#0}|{<fe#cnt#0> 1 AS cnt#0 UNION 2 AS ppp#2}}}|{<f0> tableName: join}|{<cnt#0> cnt#0}}"]
    fields.entrySet().forEach(e=>{
      val strBuffer = new StringBuffer()
      strBuffer.append("\t")
      strBuffer.append(e.getKey)
      strBuffer.append(" [label=\"")
      strBuffer.append(e.getValue)
      strBuffer.append("\"]")
      gv.addln(strBuffer.toString)
    })
    //struct0:"cnt#0" -> struct1:"f#cnt#0"
    edges.forEach(e=>{
      val keys: Array[String] = e.getKey.split(":")
      val values: Array[String] = e.getValue.split(":")
      val strBuffer = new StringBuffer()
      strBuffer.append("\t")
      strBuffer.append(keys(0))
      strBuffer.append(":\"")
      strBuffer.append(keys(1))
      strBuffer.append("\" -> ")
      strBuffer.append(values(0))
      strBuffer.append(":\"")
      strBuffer.append(values(1))
      strBuffer.append("\"")
      gv.addln(strBuffer.toString)
    })
    gv.addln(gv.end_graph)
    gv.decreaseDpi()
    gv.decreaseDpi()
    val out = new File(fileName + "." + dotFormat)
    gv.writeGraphToFile(gv.getGraph(gv.getDotSource, dotFormat), out)
  }

}
object MainZhunter {

  def getParserStruct(plan: LogicalPlan, fieldName: String, tableName: String): LineageFieldGraphVizParser = {

    val table: LineageTable = new LineageTableParser().createTableGraph(plan)
    val insertParser = new InsertTableParser()
    val parser = new LineageFieldGraphVizParser()
    val offset: Int = insertParser.parserInsertTable(plan, tableName, fieldName)
    println(s"full lineageTable: $table")
    if (offset != -1) {
      parser.createFieldString(table, offset)
    } else {
      var fieldNeed = ""
      for (meta <- table.getMetaField()) {
        val fieldCp: String = meta.getFieldName()
        if (fieldCp.split("#")(0).equals(fieldName.trim)) {
          fieldNeed = fieldCp
        }
      }
      parser.createFieldString(table, fieldNeed)
    }
    parser
  }
}
