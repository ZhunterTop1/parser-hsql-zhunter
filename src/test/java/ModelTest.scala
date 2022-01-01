import com.zhunter.model.{LineageField, LineageTable, MetaField}
import org.junit.Test

class ModelTest {
  @Test
  def testLineageField(): Unit = {
    val fieldA = new LineageField("a", "sum(b)")
    val fieldC = new LineageField("c", "sum(a)")
    fieldC.addRefer(fieldA)
    assert(fieldC.refer.length == 1, "引用字段数目错误")
  }
  
  @Test
  def testLineageTable(): Unit ={
    val table = new LineageTable("ea_stg", "stg_test")
    val fieldA = new MetaField("a", "")
    table.addMetaField(fieldA)
    println(table)
  }
  // select xxx from yyy where date = '${date}'
}
