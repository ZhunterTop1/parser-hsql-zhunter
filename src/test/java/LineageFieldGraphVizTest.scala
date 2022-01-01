import javafx.util
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.junit.Test
import com.zhunter.driver.SparkSessionDriver
import com.zhunter.parser.LineageFieldGraphVizParser

import java.util
import scala.collection.mutable

class LineageFieldGraphVizTest {
  @Test
  def testLineageField(): Unit ={
    val select =
      """
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
        |   4 as xxx)
        |where cnt != 2
        |""".stripMargin
    val plan: LogicalPlan = SparkSessionDriver.acceptLogicalPlan(select)
    println(plan.toString())
    val parser = new LineageFieldGraphVizParser()
    parser.createFieldString(plan, "cnt#4")
//    print(table)
  }
  @Test
  def testLineageJoinField(): Unit ={
    val select =
      """
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
    val plan: LogicalPlan = SparkSessionDriver.acceptLogicalPlan(select)
    println(plan.toString())
    val parser = new LineageFieldGraphVizParser()
    parser.createFieldString(plan, "cnt#5")
    println(parser.getStructField())
    println(parser.getEdges())
  }
}











