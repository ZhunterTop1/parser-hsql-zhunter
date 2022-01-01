import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.junit.Test
import com.zhunter.driver.SparkSessionDriver
import com.zhunter.model.LineageTable
import com.zhunter.parser.LineageTableParser

class InsertTableParserTest {
  @Test
  def testLineageInsert(): Unit ={
    val select =
      s"""
         |insert overwrite table AAA partition(date='xx')
         |select
         | sum(cnt) as cnt
         | ,7 as quit
         |from
         |(select
         |   1 as cnt,
         |   7 as qqq
         |union all
         |select
         |   2 as ppp,
         |   4 as xxx) a
         |group by cnt
         |""".stripMargin
    //    com.bytedance.driver.spark.sqlContext.sql()
    val plan: LogicalPlan = SparkSessionDriver.acceptLogicalPlan(select)
    println(plan.toString())
    val parser = new LineageTableParser()
    val table: LineageTable = parser.createTableGraph(plan)
//    com.bytedance.parser.parserInsertTable(plan)
    print(table)
  }
}
