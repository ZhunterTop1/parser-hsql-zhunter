import com.zhunter.MainZhunter
import com.zhunter.MainZhunter.getParserStruct
import com.zhunter.driver.SparkSessionDriver
import com.zhunter.graph.GraphViz
import com.zhunter.parser.LineageFieldGraphVizParser

import java.util.HashMap
import java.util.HashSet
import javafx.util.Pair
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.junit.Test

import java.io.File
import java.util

class FullLifeTimeTest {
  val sql = """
            |select
            | sum(cnt) as cnt,
            | sum(qqq) as num,
            | 7 as quit
            |from
            |(select
            |   1 as cnt,
            |   7 as qqq
            |union all
            |select
            |   2 as ppp,
            |   4 as xxx) a
            |left join (
            |select
            |   1 as unit
            |) b
            |on 1=1
            |""".stripMargin
  @Test
  def test2(): Unit ={
    val fieldName = "cnt"
    val tableName = "XXX"
    val zhunter = new MainZhunter()
    zhunter.createImg(null, sql, fieldName, tableName)
  }

  @Test
  def test(): Unit ={
    val fieldName = "cnt"
    val tableName = "XXX"
    val split: Array[String] = sql.split("\\s*;\\s*(?=([^']*'[^']*')*[^']*$)")
    var useSql = sql
    for (str <- split) {
      if (str != null && str.contains(tableName.trim)) {
        useSql = str
      }
    }
    val plan: LogicalPlan = SparkSessionDriver.acceptLogicalPlan(useSql)
    val parser: LineageFieldGraphVizParser = getParserStruct(plan, fieldName, tableName)
    val fields: HashMap[String, String] = parser.getStructField()
    val edges: HashSet[Pair[String, String]] = parser.getEdges()
    val dotFormat = "pdf"
    val fileName = "fullTimeTest"
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
