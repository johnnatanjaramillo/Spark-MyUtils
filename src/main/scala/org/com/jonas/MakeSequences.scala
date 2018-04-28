package org.com.jonas

import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.expressions.UserDefinedFunction

object MakeSequences {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val sparkSession = SparkSession.builder.appName("Spark-MakeSequences").getOrCreate()

    val To_Pair: UserDefinedFunction = udf((l: Long, i: Int) => l.toString + "," + i.toString + ";")
    val generatesequence = new GenerateSequence

    sparkSession.read.csv(args(0)).sample(withReplacement = false, fraction = args(1).toDouble)
      .filter("_c0 is not null")
      .withColumnRenamed("_c0", "F_WobNum")
      .withColumnRenamed("_c1", "F_SeqNumber")
      .withColumnRenamed("_c2", "value")
      .withColumnRenamed("_c3", "cluster")
      .withColumn("pair", To_Pair(col("F_SeqNumber"), col("cluster")))
      .groupBy("F_WobNum").agg(generatesequence(col("pair")).as("sequence"))
      .write.format("com.databricks.spark.csv").save(args(2))

    sparkSession.read.csv(args(3)).sample(withReplacement = false, fraction = args(4).toDouble)
      .filter("_c0 is not null")
      .withColumnRenamed("_c0", "F_WobNum")
      .withColumnRenamed("_c1", "F_SeqNumber")
      .withColumnRenamed("_c2", "value")
      .withColumnRenamed("_c3", "cluster")
      .withColumn("pair", To_Pair(col("F_SeqNumber"), col("cluster")))
      .groupBy("F_WobNum").agg(generatesequence(col("pair")).as("sequence"))
      .write.format("com.databricks.spark.csv").save(args(5))

    sparkSession.stop()

  }

  class GenerateSequence extends UserDefinedAggregateFunction {
    // This is the input fields for your aggregate function.
    override def inputSchema: org.apache.spark.sql.types.StructType =
      StructType(StructField("value", StringType) :: Nil)

    // This is the internal fields you keep for computing your aggregate.
    override def bufferSchema: StructType = StructType(
      StructField("list", StringType) :: Nil)

    // This is the output type of your aggregatation function.
    override def dataType: DataType = StringType

    override def deterministic: Boolean = true

    // This is the initial value for your buffer schema.
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = ""
    }

    // This is how to update your buffer schema given an input.
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer(0) + input.getAs[String](0)
    }

    // This is how to merge two objects with the bufferSchema type.
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getAs[String](0) + buffer2.getAs[String](0)
    }

    // This is where you output the final value, given the final value of your bufferSchema.
    override def evaluate(buffer: Row): Any = {
      var result = ""
      buffer.getAs[String](0).split(";")
        .map(t => (t.split(",")(0).toLong, t.split(",")(1).toInt))
        .sortWith(_._1 < _._1).foreach(t => result = result + t._2.toString + ";")
      result
    }
  }


}


