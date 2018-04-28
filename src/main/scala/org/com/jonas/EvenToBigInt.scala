package org.com.jonas

import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction

object EvenToBigInt {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val sparkSession = SparkSession.builder.appName("Spark-EvenToBigInt").getOrCreate()

    val vwlogsDF: DataFrame = sparkSession.read.json(args(0)).sample(withReplacement = false, fraction = args(1).toDouble)

    vwlogsDF.select(
      col("F_WobNum"),
      col("F_SeqNumber"),
      To_Binary_To_BigInt(
        col("F_WorkSpaceId"),
        col("F_OperationId"),
        col("F_WorkClassId"),
        col("F_WPClassId"),
        col("F_EventType"),
        col("F_Originator"),
        col("F_Subject"),
        col("F_Response")).as("value"))
      .write.format("com.databricks.spark.csv").save(args(2))

    import java.io._
    val fw = new FileWriter(args(3), true)
    vwlogsDF.select(
      To_Binary_To_BigInt(
        col("F_WorkSpaceId"),
        col("F_OperationId"),
        col("F_WorkClassId"),
        col("F_WPClassId"),
        col("F_EventType"),
        col("F_Originator"),
        col("F_Subject"),
        col("F_Response")).as("value"))
      .distinct().collect().foreach(e => fw.write(e.getAs[String]("value") + "\n") )
    fw.close()

    sparkSession.stop()
  }

  val To_Binary_To_BigInt: UserDefinedFunction = udf(
    (F_WorkSpaceId: Long, F_OperationId: Long, F_WorkClassId: Long, F_WPClassId: Long,
     F_EventType: Long, F_Originator: Long, F_Subject: String, F_Response: String) => {
      var result = ""

      F_WorkSpaceId match {
        case 7384 => result += "100000000000000000000"
        case 8608 => result += "010000000000000000000"
        case 8761 => result += "001000000000000000000"
        case 9861 => result += "000100000000000000000"
        case 10555 => result += "000010000000000000000"
        case 11368 => result += "000001000000000000000"
        case 11480 => result += "000000100000000000000"
        case 11749 => result += "000000010000000000000"
        case 12150 => result += "000000001000000000000"
        case 12186 => result += "000000000100000000000"
        case 12824 => result += "000000000010000000000"
        case 12916 => result += "000000000001000000000"
        case 16305 => result += "000000000000100000000"
        case 17106 => result += "000000000000010000000"
        case 17432 => result += "000000000000001000000"
        case 17826 => result += "000000000000000100000"
        case 18369 => result += "000000000000000010000"
        case 19529 => result += "000000000000000001000"
        case 20305 => result += "000000000000000000100"
        case 20417 => result += "000000000000000000010"
        case 20730 => result += "000000000000000000001"
        case _ => result += "000000000000000000000"
      }

      F_OperationId match {
        case -1 => result += "1000000000000000000000000000"
        case 0 => result += "0100000000000000000000000000"
        case 1 => result += "0010000000000000000000000000"
        case 4 => result += "0001000000000000000000000000"
        case 5 => result += "0000100000000000000000000000"
        case 6 => result += "0000010000000000000000000000"
        case 7 => result += "0000001000000000000000000000"
        case 8 => result += "0000000100000000000000000000"
        case 9 => result += "0000000010000000000000000000"
        case 10 => result += "0000000001000000000000000000"
        case 11 => result += "0000000000100000000000000000"
        case 12 => result += "0000000000010000000000000000"
        case 14 => result += "0000000000001000000000000000"
        case 17 => result += "0000000000000100000000000000"
        case 18 => result += "0000000000000010000000000000"
        case 20 => result += "0000000000000001000000000000"
        case 23 => result += "0000000000000000100000000000"
        case 24 => result += "0000000000000000010000000000"
        case 33 => result += "0000000000000000001000000000"
        case 34 => result += "0000000000000000000100000000"
        case 35 => result += "0000000000000000000010000000"
        case 37 => result += "0000000000000000000001000000"
        case 38 => result += "0000000000000000000000100000"
        case 39 => result += "0000000000000000000000010000"
        case 42 => result += "0000000000000000000000001000"
        case 63 => result += "0000000000000000000000000100"
        case 87 => result += "0000000000000000000000000010"
        case 104 => result += "0000000000000000000000000001"
        case _ => result += "0000000000000000000000000000"
      }

      F_WorkClassId match {
        case 48 => result += "100000"
        case 49 => result += "010000"
        case 50 => result += "001000"
        case 51 => result += "000100"
        case 71 => result += "000010"
        case 72 => result += "000001"
        case _ => result += "000000"
      }

      F_WPClassId match {
        case -9 => result += "1000000000000000000000000000000"
        case -7 => result += "0100000000000000000000000000000"
        case -5 => result += "0010000000000000000000000000000"
        case -1 => result += "0001000000000000000000000000000"
        case 0 => result += "0000100000000000000000000000000"
        case 1 => result += "0000010000000000000000000000000"
        case 18 => result += "0000001000000000000000000000000"
        case 19 => result += "0000000100000000000000000000000"
        case 21 => result += "0000000010000000000000000000000"
        case 24 => result += "0000000001000000000000000000000"
        case 26 => result += "0000000000100000000000000000000"
        case 27 => result += "0000000000010000000000000000000"
        case 28 => result += "0000000000001000000000000000000"
        case 29 => result += "0000000000000100000000000000000"
        case 30 => result += "0000000000000010000000000000000"
        case 31 => result += "0000000000000001000000000000000"
        case 32 => result += "0000000000000000100000000000000"
        case 33 => result += "0000000000000000010000000000000"
        case 34 => result += "0000000000000000001000000000000"
        case 36 => result += "0000000000000000000100000000000"
        case 38 => result += "0000000000000000000010000000000"
        case 39 => result += "0000000000000000000001000000000"
        case 40 => result += "0000000000000000000000100000000"
        case 41 => result += "0000000000000000000000010000000"
        case 43 => result += "0000000000000000000000001000000"
        case 44 => result += "0000000000000000000000000100000"
        case 46 => result += "0000000000000000000000000010000"
        case 58 => result += "0000000000000000000000000001000"
        case 69 => result += "0000000000000000000000000000100"
        case 70 => result += "0000000000000000000000000000010"
        case 101 => result += "0000000000000000000000000000001"
        case _ => result += "0000000000000000000000000000000"
      }

      F_EventType match {
        case 100 => result += "10000000000000000000000"
        case 120 => result += "01000000000000000000000"
        case 140 => result += "00100000000000000000000"
        case 160 => result += "00010000000000000000000"
        case 165 => result += "00001000000000000000000"
        case 170 => result += "00000100000000000000000"
        case 172 => result += "00000010000000000000000"
        case 190 => result += "00000001000000000000000"
        case 350 => result += "00000000100000000000000"
        case 352 => result += "00000000010000000000000"
        case 360 => result += "00000000001000000000000"
        case 365 => result += "00000000000100000000000"
        case 370 => result += "00000000000010000000000"
        case 380 => result += "00000000000001000000000"
        case 382 => result += "00000000000000100000000"
        case 384 => result += "00000000000000010000000"
        case 386 => result += "00000000000000001000000"
        case 390 => result += "00000000000000000100000"
        case 400 => result += "00000000000000000010000"
        case 405 => result += "00000000000000000001000"
        case 407 => result += "00000000000000000000100"
        case 500 => result += "00000000000000000000010"
        case 510 => result += "00000000000000000000001"
        case _ => result += "00000000000000000000000"
      }

      F_Originator match {
        case 0 => result += "1000"
        case 51 => result += "0100"
        case 137 => result += "0010"
        case 200 => result += "0001"
        case _ => result += "0000"
      }

      F_Subject match {
        case "" => result += "100000000"
        case "JD TEST Presolicitud de Emision" => result += "010000000"
        case "Presolicitud de Emision" => result += "001000000"
        case "Proceso Cargue Masivo Emision" => result += "000100000"
        case "Proceso Emision Otras Actividades" => result += "000010000"
        case "Proceso de Control de Documentos" => result += "000001000"
        case "Proceso de Emision" => result += "000000100"
        case "Proceso de Suscripcion" => result += "000000010"
        case "Prueba JD Presolicitud de Emision" => result += "000000001"
        case _ => result += "000000000"
      }

      F_Response match {
        case "" => result += "10000000000000000000000000000"
        case "ACEPTAR" => result += "01000000000000000000000000000"
        case "ATAR" => result += "00100000000000000000000000000"
        case "Aceptar" => result += "00010000000000000000000000000"
        case "Aprobar" => result += "00001000000000000000000000000"
        case "Asignar" => result += "00000100000000000000000000000"
        case "AvanzarPaso" => result += "00000010000000000000000000000"
        case "CARGUE MASIVO" => result += "00000001000000000000000000000"
        case "Cancelar" => result += "00000000100000000000000000000"
        case "Completa" => result += "00000000010000000000000000000"
        case "ConError" => result += "00000000001000000000000000000"
        case "DECLINAR" => result += "00000000000100000000000000000"
        case "Declinar" => result += "00000000000010000000000000000"
        case "Emitida" => result += "00000000000001000000000000000"
        case "Escalar" => result += "00000000000000100000000000000"
        case "EscalarExpreso" => result += "00000000000000010000000000000"
        case "EscalarSise" => result += "00000000000000001000000000000"
        case "Incompleta" => result += "00000000000000000100000000000"
        case "Liberar" => result += "00000000000000000010000000000"
        case "RECHAZAR" => result += "00000000000000000001000000000"
        case "Rechazar" => result += "00000000000000000000100000000"
        case "RechazarSolicitud" => result += "00000000000000000000010000000"
        case "Recotizar" => result += "00000000000000000000001000000"
        case "Reintentar" => result += "00000000000000000000000100000"
        case "Revisar" => result += "00000000000000000000000010000"
        case "SUSPENDER" => result += "00000000000000000000000001000"
        case "TERMINAR" => result += "00000000000000000000000000100"
        case "Terminar" => result += "00000000000000000000000000010"
        case "Terminar OK" => result += "00000000000000000000000000001"
        case _ => result += "00000000000000000000000000000"
      }
      BigInt(result, 2).toString()
    })

}

