package com.zhunter.model

import scala.collection.mutable.ArrayBuffer

class MetaTable (var databaseName: String, var tableName: String, var field: ArrayBuffer[MetaField] = new ArrayBuffer[MetaField](), var where: String=null, var group: String=null){
  def setDatabaseName(databaseName: String): Unit ={
    this.databaseName = databaseName
  }
  def getDatabaseName(): String ={
    this.databaseName
  }
  def setTableName(tableName: String): Unit ={
    this.tableName = tableName
  }
  def getTableName(): String = {
    this.tableName
  }
  def addMetaField(metaField: MetaField): Unit ={
    this.field.append(metaField)
  }
  def getMetaField(): ArrayBuffer[MetaField] ={
    this.field
  }
  def setWhere(where: String): Unit ={
    this.where = where
  }
  def getWhere():String ={
    this.where
  }
  def setGroup(group: String): Unit ={
    this.group = group
  }
  def getGroup(): String ={
    this.where
  }
  override def toString: String = {
    if(where != null){
      f"databaseName: $databaseName; tableName: $tableName; where: $where"
    }else{
      f"databaseName: $databaseName; tableName: $tableName"
    }
  }
}

class MetaField (var fieldName: String, var expression: String, var reference: ArrayBuffer[String] = new ArrayBuffer[String]()){
  def setFieldName(fieldName: String): Unit ={
    this.fieldName = fieldName
  }
  def getFieldName(): String ={
    fieldName
  }
  def setExpression(expression: String): Unit ={
    this.expression = expression
  }
  def getExpression(): String = {
    this.expression
  }
  def addReference(refer: String): Unit ={
    this.reference.append(refer)
  }
  def getReference(): ArrayBuffer[String] ={
    this.reference
  }
  override def clone(): MetaField = {
    new MetaField(fieldName, expression, reference.clone())
  }
  override def toString: String = {
    f"fieldName: $fieldName, expression: [$expression], reference: [${reference.toString()}]"
  }
}

class LineageTable(var databaseName: String=null, var tableName: String=null,
                   var field: ArrayBuffer[MetaField]=new ArrayBuffer[MetaField](),
                   var childrenTable: ArrayBuffer[LineageTable]=new ArrayBuffer[LineageTable](), var where: String=null,
                   var group: String=null)
//  extends MetaTable(databaseName, tableName, field)
{
  def setDatabaseName(databaseName: String): Unit ={
    this.databaseName = databaseName
  }
  def getDatabaseName(): String ={
    this.databaseName
  }
  def setTableName(tableName: String): Unit ={
    this.tableName = tableName
  }
  def getTableName(): String = {
    this.tableName
  }
  def addMetaField(metaField: MetaField): Unit ={
    this.field.append(metaField)
  }
  def getMetaField(): ArrayBuffer[MetaField] ={
    this.field
  }
  def addChildrenTable(table: LineageTable): Unit ={
    this.childrenTable.append(table)
  }
  def getChildrenTable(): ArrayBuffer[LineageTable] ={
    this.childrenTable
  }
  def setWhere(where: String): Unit ={
    this.where = where
  }
  def getWhere():String ={
    this.where
  }
  def setGroup(group: String): Unit ={
    this.group = group
  }
  def getGroup(): String ={
    this.where
  }
  override def toString: String = {
    toString(1)
  }
  def toString(deep: Int): String = {
    var fields = ""
    for(oneField <- field){
      fields += oneField + ";"
    }
    var str = if(where == null) f"databaseName: $databaseName; tableName: $tableName; fields: [$fields]\n"
    else f"databaseName: $databaseName; tableName: $tableName; where: $where; fields: [$fields]\n"
    for(children: LineageTable <- childrenTable){
      str+="\t"*deep + " " + children.toString(deep+1)
    }
    str
  }
  override def clone(): LineageTable = {
    new LineageTable(databaseName, tableName,where=where, group=group)
  }
}

class LineageField(var fieldName: String, var expression: String, var table: LineageTable = null,
                   var refer: ArrayBuffer[LineageField]=new ArrayBuffer[LineageField]())
//  extends MetaField(fieldName, expression, refer)
{
  def setTable(table: LineageTable): Unit ={
    this.table = table
  }
  def getTable(): LineageTable ={
    this.table
  }
  def addRefer(lineageField: LineageField): Unit ={
    this.refer.append(lineageField)
  }
  def getRefer(): ArrayBuffer[LineageField]={
    this.refer
  }
  override def toString: String = {
    f"fieldName: $fieldName; expression: [$expression]; table: $table; referLen: ${refer.length}"
  }
}













