name := "spark3example"

version := "0.1"

scalaVersion := "2.12.12"

val sparkCore =  "org.apache.spark" %% "spark-core" % "3.0.0"
val sparkSql =   "org.apache.spark" %% "spark-sql" % "3.0.0"
val sparkHive =  "org.apache.spark" %% "spark-hive" % "3.0.0"

libraryDependencies ++= Seq(sparkCore,sparkSql,sparkHive)