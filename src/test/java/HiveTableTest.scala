import com.zhunter.driver.SparkSessionDriver
import org.apache.spark.sql.SparkSession
import org.junit.Test

class HiveTableTest {
  @Test
  def testHiveUse(): Unit ={
    SparkSessionDriver.init()
    val spark: SparkSession = SparkSessionDriver.spark
    val createSql =
      """|
         |create table ea_stg.A
         |as
         |select
         |  1 as cnt
         |""".stripMargin
    spark.sql("show databases").show()
    spark.sql("create database if not exists ea_stg")
    spark.sql("show databases").show()
    spark.sql(createSql)
    spark.sql("use ea_stg")
    spark.sql("show tables").show()
  }
}
