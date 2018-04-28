package org.com.jonas

import org.apache.log4j.{Level, Logger}

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object EventSetCluster {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val sparkSession = SparkSession.builder.appName("Spark-EventSetCluster").getOrCreate()
    val vwlogsDF: DataFrame = sparkSession.read.csv(args(0)).sample(withReplacement = false, fraction = args(1).toDouble)
      .withColumnRenamed("_c0", "F_WobNum")
      .withColumnRenamed("_c1", "F_SeqNumber")
      .withColumnRenamed("_c2", "value")

    import scala.io.Source
    val centroidsModel: Array[String] = Source.fromFile(args(2)).getLines.toArray
    val sc: SparkContext = sparkSession.sparkContext
    val centroids = sc.broadcast(centroidsModel)
    val minhdistance = udf((s: String) =>
      centroids.value.zipWithIndex.map(c => (hamming_distance(BigInt(s), BigInt(c._1)), c._2)).minBy(_._1)._2)
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

