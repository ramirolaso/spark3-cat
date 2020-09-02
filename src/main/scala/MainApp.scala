import java.io.File
import java.sql.Timestamp

import com.mycatalog.SuperSessionManager
import org.apache.hadoop.conf.Configuration
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.{SparkConf, Success}
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH
import org.joda.time.{DateTime, IllegalInstantException}
import org.joda.time.format.ISODateTimeFormat

import scala.::
import scala.util.{Failure, Random, Success, Try}

case class Persona (nombre:String, apellido:String,dni:Int,cat:String)


object MainApp {



  def createPersona(year:Int,month:Int,day:Int,num:Int):Option[Persona] = {
    try{
      val cats = "A"::"B"::"C"::"D"::Nil
      Option(Persona(NameGen.name,NameGen.name,num,Random.shuffle(cats).head))
    }catch {
      case e:IllegalInstantException => None
  }
  }
   def main(args: Array[String]): Unit = {

    val people = new scala.collection.mutable.ArrayBuffer[Persona]()

    for (year <- Range(2000,2020)){
      print(s" year: $year")
      for(month <-Range(1,12)){
        for(day <- Range(1,28)){
          for(num <- Range(1,100))
            {
              createPersona(year,month,day,num).foreach(p=>people.append(p))
            }

        }
      }
    }

    println(s"created ${people.size}")



    val session = SuperSessionManager.session


    val df = session.createDataFrame(people).toDF()

    val table = df.writeTo("MyCatalog.pruebaaa").partitionedBy(col("cat"))
    table.createOrReplace()
    //session.sql("show tables from MyCatalog ").show(false)
    //session.sql("drop table MyCatalog.blabla ").show(false)



    session.close()



  }
}
