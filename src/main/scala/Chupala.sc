import java.net.URI

import com.mycatalog.SuperSessionManager
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

val session = SuperSessionManager.session
session.sql("show tables from RamiCatalog2").show(false)
val mydf =session.read.parquet("hdfs://localhost:8020/personas")
mydf.show(1000)