package feedback

import java.nio.file.Paths

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
    val initDf = read("feedback.csv")
    val finalDf = feedbackGrouped(initDf)
    val finalSqlDf = feedbackGroupedSql(initDf)

    val initDs = feedbackSummaryTyped(initDf)
    val finalDs = feedbackGroupedTyped(initDs)

    finalDs.show()

    spark.close()
  }

  def read(resource: String): DataFrame = {
    val rdd = spark.sparkContext.textFile(fsPath(resource))

    val headerColumns = rdd.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfSchema(headerColumns)

    val data =
      rdd
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(row)

    spark.createDataFrame(data, schema)
  }

  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

  def dfSchema1(columnNames: List[String]): StructType = {
    def dfField(columnName: String): StructField = columnName match {
      case strCol @ ("manager_name" | "client_name" | "client_sex") =>
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
        StructField(name = "client_sex", dataType = StringType, nullable = false),
        StructField(name = "client_age", dataType = IntegerType, nullable = false),
        StructField(name = "response_time", dataType = DoubleType, nullable = false),
        StructField(name = "statisfaction_level", dataType = DoubleType, nullable = false)
      )
    )
  }

  def row1(line: List[String]): Row = line match {
    case List(mName, cName, cSex, cAge, rTime, sLevel) =>
      Row(mName, cName, cSex, cAge.toInt, rTime.toDouble, sLevel.toDouble)
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
        round(avg($"statisfaction_level"), 1).as("statisfaction")
      )
      .orderBy($"statisfaction")
  }

  def feedbackGroupedSql(summed: DataFrame): DataFrame = {
    val viewName = s"summed"

    val sql =
      s"""
        SELECT manager_name,
        ROUND(SUM(response_time) / COUNT(response_time), 1) AS time,
        ROUND(SUM(statisfaction_level) / COUNT(statisfaction_level), 1) AS statisfaction
        FROM $viewName
        GROUP BY manager_name
        ORDER BY statisfaction
      """

    summed.createOrReplaceTempView(viewName)
    spark.sql(sql)
  }

  def feedbackSummaryTyped(feedbackSummaryDf: DataFrame): Dataset[FeedbackRow] = {
    feedbackSummaryDf.as[FeedbackRow]
  }

  def feedbackGroupedTyped(summed: Dataset[FeedbackRow]): Dataset[FeedbackRow] = {
    import spark.implicits._

    def scaledAvg[IN](f: IN => Double): TypedColumn[IN, Double] = new TypedScaledAverage(f).toColumn

    summed
      .groupByKey(x => x.manager_name)
      .agg(scaledAvg(_.response_time), scaledAvg(_.statisfaction_level))
      .map { case (managerName, time, statisfaction) =>
        FeedbackRow(managerName, time, statisfaction)
      }.orderBy($"statisfaction_level")
  }

}

case class FeedbackRow(manager_name: String, response_time: Double, statisfaction_level: Double)

class TypedScaledAverage[IN](f: IN => Double) extends TypedAverage[IN](f) {
  override def finish(red: (Double, Long)): Double = (red._1 / red._2 * 10).round / 10d
}