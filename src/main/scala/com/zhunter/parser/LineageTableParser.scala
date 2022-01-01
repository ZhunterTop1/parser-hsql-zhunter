package com.zhunter.parser

import com.zhunter.model.{LineageTable, MetaField}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, InsertIntoStatement, Join, LogicalPlan, OneRowRelation, Project, SubqueryAlias, Union}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable

class LineageTableParser {
  def createTableGraph(tree: LogicalPlan): LineageTable = {

    if(tree.children.isEmpty){
      var lineageTable: LineageTable = null
      tree match {
        case oneRow: OneRowRelation =>{
          lineageTable = new LineageTable(null, oneRow.toString().trim())
        }
        case relation: LogicalRelation =>{
          lineageTable = new LineageTable(null, null)
          relation.output.map(ele => {
            lineageTable.addMetaField(new MetaField(ele.toString(), ele.toString()))
          })
        }
        case unresolvedRelation: UnresolvedRelation =>{
          val tableName: String = unresolvedRelation.multipartIdentifier.toString().trim
          lineageTable = new LineageTable(null, tableName)
        }
      }
      return lineageTable
    }

    if(tree.children.size == 1){
      val preLineageTable = createTableGraph(tree.children(0))
      var table: LineageTable = preLineageTable
      tree match {
        case proj: Project =>{
          if(preLineageTable.getMetaField().size != 0){
            table = new LineageTable(null, null)
            table.addChildrenTable(preLineageTable)
          }
          proj.projectList.map(ele => {
            if(ele.isInstanceOf[UnresolvedAlias]){
              val alias: UnresolvedAlias = ele.asInstanceOf[UnresolvedAlias]
              val field = new MetaField(alias.toString().trim, alias.toString().trim)
              alias.children.foreach(e => field.addReference(e.toString()))
              table.addMetaField(field)
            }else{
              val field = new MetaField(ele.name + "#" + ele.exprId.id, ele.toString())
              ele.references.foreach(e => field.addReference(e.toString()))
              table.addMetaField(field)
            }
          })
        }
        case subQuery: SubqueryAlias =>{
          if(table.getTableName() != null){
            table = new LineageTable(null, null)
            table.addChildrenTable(preLineageTable)
          }
          if(table.getChildrenTable().size == 1){
            table.getChildrenTable()(0).getMetaField().foreach(ele=>{
              val field: MetaField = ele.clone()
              field.reference.clear()
              field.addReference(field.fieldName)
              table.addMetaField(field)
            })
          }
          table.setTableName(subQuery.getClass.getSimpleName + "." + subQuery.identifier.toString().trim)
        }
        case agg: Aggregate => {
          table = new LineageTable(null, null)
          table.addChildrenTable(preLineageTable)
          agg.aggregateExpressions.map(ele => {
            if(ele.isInstanceOf[UnresolvedAlias]){
              val alias: UnresolvedAlias = ele.asInstanceOf[UnresolvedAlias]
              val field = new MetaField(alias.toString().trim, alias.toString().trim)
              alias.children.foreach(e => field.addReference(e.toString()))
              table.addMetaField(field)
            }else{
              val field = new MetaField(ele.name + "#" + ele.exprId.id, ele.toString())
              ele.references.foreach(e=>field.addReference(e.toString()))
              table.addMetaField(field)
            }
          })
        }
        case filter: Filter =>{
          table.setWhere(filter.condition.toString())
        }
        case statement: InsertIntoStatement =>{
          val tableName: String = statement.table.toString()
          val cols: Seq[String] = statement.userSpecifiedCols
          preLineageTable.setTableName(tableName.trim)
          table = preLineageTable
        }
        case hiveTable: InsertIntoHiveTable =>{
          val hiveTableName = hiveTable.table.identifier.database.head + "." + hiveTable.table.identifier.table
          preLineageTable.setTableName(hiveTableName.trim)
          table = preLineageTable
        }
      }

      return table
    }

    if(tree.children.size == 2){
      val headLineageTable = createTableGraph(tree.children(0))
      val lastLineageTable = createTableGraph(tree.children(1))
      val emptyTable = new LineageTable(null, null)
      emptyTable.addChildrenTable(headLineageTable)
      emptyTable.addChildrenTable(lastLineageTable)
      tree match {
        case _: Union => {
          val headFields = headLineageTable.getMetaField()
          val lastFields = lastLineageTable.getMetaField()
          val size = headLineageTable.getMetaField().size
          for(i <- 0 until size){
            val metaField = new MetaField(headFields(i).fieldName, headFields(i).expression + " UNION " + lastFields(i).expression)
            metaField.addReference(headFields(i).fieldName)
            metaField.addReference(lastFields(i).fieldName)
            emptyTable.addMetaField(metaField)
          }
        }
        case _: Join =>{
          emptyTable.setTableName("join")
          headLineageTable.getMetaField().foreach(ele=>{
            val field: MetaField = ele.clone()
            field.reference.clear()
            field.addReference(field.fieldName)
            emptyTable.addMetaField(field)
          })
          lastLineageTable.getMetaField().foreach(ele=>{
            val field: MetaField = ele.clone()
            field.reference.clear()
            field.addReference(field.fieldName)
            emptyTable.addMetaField(field)
          })
        }
      }
      return emptyTable
    }
    null
  }
}
