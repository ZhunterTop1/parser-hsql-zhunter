import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.junit.Test
import com.zhunter.driver.SparkSessionDriver
import com.zhunter.model.LineageTable
import com.zhunter.parser.{LineageFieldParser, LineageTableParser}

class LineageTableTest {
  @Test
  def testLineageTable(): Unit ={
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
//    com.bytedance.driver.spark.sqlContext.sql()
    val plan: LogicalPlan = SparkSessionDriver.acceptLogicalPlan(select)
    println(plan.toString())
    val parser = new LineageTableParser()
    val table: LineageTable = parser.createTableGraph(plan)
    print(table)
  }
  @Test
  def testLineageTable2(): Unit ={
    val select =
      """
        |WITH dm_accountant_time AS
        |(
        |    SELECT accountant_time AS atm
        |    ,regexp_replace(accountant_time,'-','') AS atmd
        |    ,lastdate_accountant_time AS lastdate_atm
        |    FROM ea_dm.dm_accountant_time
        |),
        |uniq_key_construct AS
        |(
        |    -- https://bytedance.feishu.cn/docs/doccn6WZIOHGeitjMCbN7fAYOVe
        |    -- 见<<校验流水逻辑>>部分
        |    SELECT
        |        CONCAT(bank_db_urid) AS  testing,
        |        COUNT(1)             AS  ct1
        |    FROM
        |        ea_stg.stg_revenue_third_prd_payment_third_bank_da
        |    JOIN
        |        dm_accountant_time
        |    ON
        |        1=1
        |    WHERE
        |        SUBSTRING(date,1,6)>=atmd
        |    GROUP BY
        |        CONCAT(bank_db_urid)
        |)
        |INSERT INTO TABLE ea_rpt.rpt_fin_revenue_third_error_list_ha PARTITION (`date` = '${date}',hour='${hour}')
        |SELECT
        |    '拆账单' AS case_type
        |    ,b.testing AS case_key
        |    ,merchant_number AS merchant_number
        |    ,'汇总银行'
        |FROM
        |    ea_stg.stg_revenue_third_prd_payment_third_bank_da a
        |JOIN
        |    uniq_key_construct b
        |ON
        |    concat(a.bank_db_urid) = b.testing
        |JOIN
        |    dm_accountant_time ON 1=1 WHERE
        |    substring(a.date,1,6)>=atmd
        |AND
        |    b.ct1 > 1
        |""".stripMargin
    //    com.bytedance.driver.spark.sqlContext.sql()
    val plan: LogicalPlan = SparkSessionDriver.acceptLogicalPlan(select)
    println(plan.toString())
    val parser = new LineageTableParser()
    val table: LineageTable = parser.createTableGraph(plan)
    print(table)
  }
  @Test
  def testLineageTableJoin(): Unit ={
    val select =
      """
        |select
        | sum(a.cnt) as cnt,
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
        |   1 as cnt
        |) b
        |on a.cnt=b.cnt
        |""".stripMargin
    //    com.bytedance.driver.spark.sqlContext.sql()
    val plan: LogicalPlan = SparkSessionDriver.acceptLogicalPlan(select)
    println(plan.toString())
    val parser = new LineageTableParser()
    val table: LineageTable = parser.createTableGraph(plan)
    print(table)

  }
  @Test
  def testLineageGroupBy(): Unit ={
    val select =
      """
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
    print(table)
  }

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
    print(table)
  }



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
    //    com.bytedance.driver.spark.sqlContext.sql()
    SparkSessionDriver.init()
    val plan: LogicalPlan = SparkSessionDriver.acceptLogicalPlan(select)
    println(plan.toString())
    val parser = new LineageFieldParser()
    val table: LineageTable = parser.createFieldGraph(plan, "cnt#4")
    print(table)
  }

}











