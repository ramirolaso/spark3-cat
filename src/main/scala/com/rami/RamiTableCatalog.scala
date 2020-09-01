package com.rami

import java.util

import org.apache.spark.sql.connector.catalog.TableChange.RenameColumn
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform

import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConverters._
/**
 * Catalog implementations are registered to a name by adding a configuration option
 * to Spark: spark.sql.catalog.catalog-name=com.example.YourCatalogClass.
 * All configuration properties in the Spark configuration that share the catalog name prefix,
 * spark.sql.catalog.catalog-name.(key)=(value) will be passed in the case insensitive string map of options
 * in initialization
 * with the prefix removed. name, is also passed and is the catalog's name; in this case, "catalog-name".
 */
/**
 * http://blog.madhukaraphatak.com/spark-3-datasource-v2-part-1/
 */
class RamiTableCatalog extends DelegatingCatalogExtension{


  override def name(): String = {
    val nombre ="RamiCatalog"
   // println(nombre)
    nombre
  }

  override def defaultNamespace():Array[String] = Array("prueba")


  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = {

    partitions.foreach(p=>{
      println(s"agregando al catalogo...${p.name()}")
    })

    val p = Map("location"->"hdfs://localhost:8020/")
    val table = super.createTable(ident,schema,partitions,p.asJava)
    table

  }


  override def dropTable(ident: Identifier): Boolean = true

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {

  }
}


