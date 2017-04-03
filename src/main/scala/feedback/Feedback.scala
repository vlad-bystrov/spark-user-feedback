package feedback

import java.nio.file.Paths

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.aggregate.TypedAverage
import org.apache.spark.sql.{DataFrame, Dataset, Row, TypedColumn}
import org.apache.spark.sql.types._

/**
  * Feedback
  *
  * @author Vladimir Bystrov
  */
object Feedback {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .config("spark.master", "local")
      .getOrCreate()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {
    val rawRDD = read("feedback.csv")
    val initRDD = toRDD(rawRDD)

    val finalRDD = feedbackGroupedRDD(initRDD)

    val initDf = toDF(rawRDD)

    val finalDf = feedbackGrouped(initDf)
    val finalSqlDf = feedbackGroupedSql(initDf)

    val initDs = feedbackSummaryTyped(initDf)
    val finalDs = feedbackGroupedTyped(initDs)

    initDf.show()

    spark.close()
  }

  def read(resource: String): RDD[String] =
    spark.sparkContext.textFile(fsPath(resource))

  def toRDD(raw: RDD[String]): RDD[FeedbackRow] =
    raw
      .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it)
      .map { el =>
        val arr = el.split(",")
        FeedbackRow(arr(0), arr(1), arr(2), arr(3).toInt, arr(4).toDouble, arr(5).toDouble)
      }

  def toDF(raw: RDD[String]): DataFrame = {
    val headerColumns = raw.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfSchema(headerColumns)

    val data =
      raw
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(row)

    spark.createDataFrame(data, schema)
  }

  def feedbackGroupedRDD(rdd: RDD[FeedbackRow]): RDD[FeedbackGroupedRow] = {
    def roundAvg(value: Double): Double = (value * 10).round / 10d

    def avg(it: Iterable[FeedbackRow]): (Double, Double) = {
      def seqop(acc: (Double, Double), row: FeedbackRow): (Double, Double) =
        (acc._1 + row.response_time, acc._2 + row.satisfaction_level)

      def comboop(acc1: (Double, Double), acc2: (Double, Double)): (Double, Double) =
        (acc1._1 + acc2._1, acc1._2 + acc2._2)

      val count = it.size
      val (time, satisfaction) = it.aggregate((0.0, 0.0))(seqop, comboop)

      (time / count, satisfaction / count)
    }

    val grouped: RDD[(String, Iterable[FeedbackRow])] = rdd.map(el => (el.manager_name, el)).groupByKey()

    grouped
      .mapValues(avg)
      .map {
        case (mname, (rtime, slevel)) => FeedbackGroupedRow(mname, roundAvg(rtime), roundAvg(slevel))
      }
  }

  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

  def dfSchema1(columnNames: List[String]): StructType = {
    def dfField(columnName: String): StructField = columnName match {
      case strCol @ ("manager_name" | "client_name" | "client_gender") =>
        StructField(name = strCol, dataType = StringType, nullable = false)
      case intCol @ "client_age" =>
        StructField(name = intCol, dataType = IntegerType, nullable = false)
      case doubleCol =>
        StructField(name = doubleCol, dataType = DoubleType, nullable = false)
    }

    StructType(columnNames.map(dfField))
  }

  def dfSchema(columnNames: List[String]): StructType = {
    StructType(
      Seq(
        StructField(name = "manager_name", dataType = StringType, nullable = false),
        StructField(name = "client_name", dataType = StringType, nullable = false),
        StructField(name = "client_gender", dataType = StringType, nullable = false),
        StructField(name = "client_age", dataType = IntegerType, nullable = false),
        StructField(name = "response_time", dataType = DoubleType, nullable = false),
        StructField(name = "satisfaction_level", dataType = DoubleType, nullable = false)
      )
    )
  }

  def row1(line: List[String]): Row = line match {
    case List(mName, cName, cgender, cAge, rTime, sLevel) =>
      Row(mName, cName, cgender, cAge.toInt, rTime.toDouble, sLevel.toDouble)
    case _ => Row()
  }

  def row(line: List[String]): Row = {
    Row(line(0), line(1), line(2), line(3).toInt, line(4).toDouble, line(5).toDouble)
  }

  def feedbackGrouped(data: DataFrame): DataFrame = {
    data
      .groupBy($"manager_name")
      .agg(
        round(avg($"response_time"), 1).as("time"),
        round(avg($"satisfaction_level"), 1).as("satisfaction")
      )
      .orderBy($"satisfaction")
  }

  def feedbackGroupedSql(summed: DataFrame): DataFrame = {
    val viewName = s"summed"

    val sql =
      s"""
        SELECT manager_name,
        ROUND(SUM(response_time) / COUNT(response_time), 1) AS time,
        ROUND(SUM(satisfaction_level) / COUNT(satisfaction_level), 1) AS satisfaction
        FROM $viewName
        GROUP BY manager_name
        ORDER BY satisfaction
      """

    summed.createOrReplaceTempView(viewName)
    spark.sql(sql)
  }

  def feedbackSummaryTyped(feedbackSummaryDf: DataFrame): Dataset[FeedbackRow] = {
    feedbackSummaryDf.as[FeedbackRow]
  }

  def feedbackGroupedTyped(summed: Dataset[FeedbackRow]): Dataset[FeedbackGroupedRow] = {
    import spark.implicits._

    def scaledAvg[IN](f: IN => Double): TypedColumn[IN, Double] = new TypedScaledAverage(f).toColumn

    summed
      .groupByKey(x => x.manager_name)
      .agg(scaledAvg(_.response_time), scaledAvg(_.satisfaction_level))
      .map { case (managerName, time, satisfaction) =>
        FeedbackGroupedRow(managerName, time, satisfaction)
      }.orderBy($"satisfaction_level")
  }

}

case class FeedbackRow(manager_name: String, client_name: String, client_gender: String, client_age: Int, response_time: Double, satisfaction_level: Double)

case class FeedbackGroupedRow(manager_name: String, response_time: Double, satisfaction_level: Double)

class TypedScaledAverage[IN](f: IN => Double) extends TypedAverage[IN](f) {
  override def finish(red: (Double, Long)): Double = (red._1 / red._2 * 10).round / 10d
}