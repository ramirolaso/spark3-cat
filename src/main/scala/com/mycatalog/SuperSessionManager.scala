package com.mycatalog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

object SuperSessionManager {
  private val numberCpus = sys.env.getOrElse("NUM_CPUS", "*")

  private val builder = {

    SparkSession.builder()
      .config("spark.sql.catalog.MyCatalog", "com.mycatalog.MyCatalog")

  }


  val session = {
    val session = builder.appName(sys.env.getOrElse("LOGNAME", "no_name"))
      .master(s"local[$numberCpus]")
      .getOrCreate()

    session
  }


}
