import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

class Py4jParamTest {
  def hello(v1: Object, v2: Object)={
    println(v1)
    print(v1.getClass.getSimpleName)
    println(v2)
    print(v2.getClass.getSimpleName)
    if(v1.isInstanceOf[SparkSession] && v2.isInstanceOf[String]){
      val spark: SparkSession = v1.asInstanceOf[SparkSession]
      val sql: String = v2.asInstanceOf[String]
      val parser = spark.sessionState.sqlParser
      val plan: LogicalPlan = parser.parsePlan(sql)
      val analyzed: LogicalPlan = spark.sessionState.analyzer.execute(plan)
      println(analyzed)
    }
  }
}
