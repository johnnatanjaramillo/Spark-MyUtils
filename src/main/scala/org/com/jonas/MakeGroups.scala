package org.com.jonas

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MakeGroups {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val sparkSession = SparkSession.builder.appName("Spark-MakeGroups").getOrCreate()

    sparkSession.read.csv(args(0)).sample(withReplacement = false, fraction = args(1).toDouble)
      .withColumnRenamed("_c0", "F_WobNum")
      .withColumnRenamed("_c1", "F_SeqNumber")
      .withColumnRenamed("_c2", "value")
      .withColumnRenamed("_c3", "cluster")
      .createOrReplaceTempView("vwlogsDF")

    sparkSession.read.csv(args(2))
      .withColumnRenamed("_c0", "F_WobNum")
      .createOrReplaceTempView("vwError")

    //error
    sparkSession.sql("select vwlogsDF.* from vwlogsDF right join vwError on vwlogsDF.F_WobNum = vwError.F_WobNum")
      .write.format("com.databricks.spark.csv").save(args(3))

    //no error
    sparkSession.sql("select vwlogsDF.* from vwlogsDF left join vwError on vwlogsDF.F_WobNum = vwError.F_WobNum where vwError.F_WobNum is null")
      .write.format("com.databricks.spark.csv").save(args(4))

    sparkSession.stop()
  }

}

