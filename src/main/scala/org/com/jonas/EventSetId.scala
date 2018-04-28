package org.com.jonas

import org.apache.log4j.{Level, Logger}

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object EventSetId {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val sparkSession = SparkSession.builder.appName("Spark-EventSetId").getOrCreate()
    val vwlogsDF: DataFrame = sparkSession.read.csv(args(0)).sample(withReplacement = false, fraction = args(1).toDouble)
      .withColumnRenamed("_c0", "F_WobNum")
      .withColumnRenamed("_c1", "F_SeqNumber")
      .withColumnRenamed("_c2", "value")

    import scala.io.Source
    val events: Array[BigInt] = Source.fromFile(args(2)).getLines.toArray.map(BigInt(_)).sorted

    val sc: SparkContext = sparkSession.sparkContext
    val centroids = sc.broadcast(events)

    val minhdistance = udf((s: String) => centroids.value.indexWhere(_ == BigInt(s)))
    vwlogsDF.withColumn("cluster", minhdistance(col("value")))
      .write.format("com.databricks.spark.csv").save(args(3))

    sparkSession.stop()
  }

  def hamming_distance(v1: BigInt, v2: BigInt): Int = {
    var v = v1 ^ v2
    var c = 0
    while (v != 0) {
      v &= v - 1
      c += 1
    }
    c
  }

}

