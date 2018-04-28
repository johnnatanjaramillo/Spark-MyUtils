package org.com.jonas

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SplitCollection {
  /**
    * @param args
    * args(0): Path of collection
    * args(1): Number of files
    * args(2): Name of files
    * args(3): Path of result files
    * @return result
    *         file with structure: M;k;finalPi,finalA,finalB
    */
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val sparkSession = SparkSession.builder.appName("Spark-SplitCollection").getOrCreate()
    val myCollection: DataFrame = sparkSession.read.csv(args(0))

    val pathFiles = args(3) + "/" + args(2) + "_"
    (0 until args(1).toInt).foreach(i =>
      this.writeresult(pathFiles + i.toString, myCollection.schema.toArray.map(e => e.name).mkString(",") + "\n"))

    var temp = 0
    val tempCollection = myCollection.toLocalIterator()

    while (tempCollection.hasNext)
      (0 until args(1).toInt).foreach(i =>
        if(tempCollection.hasNext)
          this.writeresult(pathFiles + i.toString, tempCollection.next().mkString(",") + "\n"))


    sparkSession.stop()
  }

  def writeresult(path: String, result: String): Unit = {
    import java.io._
    val fw = new FileWriter(path, true)
    fw.write(result)
    fw.close()
  }

}
