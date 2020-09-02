package com.mycatalog

import java.net.URI
import java.nio.file.Paths
import java.util

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetTableUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.{FileDataSourceV2, FileTable}
import org.apache.spark.sql.execution.datasources.v2.parquet.{ParquetDataSourceV2, ParquetTable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.JavaConverters._

 class MyCatalog() extends TableCatalog{

   var option:CaseInsensitiveStringMap=null
   val ds=new MyParquetDataSource
   val hadoop = SparkSession.active.sparkContext.hadoopConfiguration
   val fs =FileSystem.get(new URI("hdfs://localhost:8020"),hadoop)
   val objectMapper = new ObjectMapper
   var tables:List[Table] = List()


   override def listTables(namespace: Array[String]): Array[Identifier] = tables.map(table => Identifier.of(Array(),table.name())).toArray

   override def loadTable(ident: Identifier): Table = {
     tables.filter((t=> t.name() == ident.name())).headOption.getOrElse(null)
   }

   override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = {
     val ops = Map("paths"->s"""["hdfs://localhost:8020/${ident.name()}"]""",
                   "tableName"->ident.name())

     val table = ds.getTable(new CaseInsensitiveStringMap(ops.asJava),schema)
     tables = tables ++ List(table)
     table
   }

   override def alterTable(ident: Identifier, changes: TableChange*): Table = ???

   override def dropTable(ident: Identifier): Boolean = {
     val table = Option(loadTable(ident))

     table.map(t => {

       val paths = Option(t.properties().get("paths")).map { pathStr =>
         objectMapper.readValue(pathStr, classOf[Array[String]])}.getOrElse(Array())

         paths.foreach(p => {
           val path = new Path(p)
           val ret = fs.delete(path,true)
           println(s"Deleted? ${p} $ret")
         })

       tables = tables.filterNot(t => t.name() == ident.name())
       println("Deleted!")
       true
     }).getOrElse(false)


   }

   override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = ???

   override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
     option = options
   }

   override def name(): String = "MyCatalog"
 }
