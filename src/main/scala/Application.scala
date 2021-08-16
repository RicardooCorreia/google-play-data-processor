import custom.Functions.{convertStringToBytes, convertToEur, mapStringToArray}
import io.FileNames
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}
import spark.SparkSessionProvider

object Application extends App {

  override def main(args: Array[String]): Unit = {

    val spark = SparkSessionProvider.getSparkSession("Google play application")

    val appsDataFrame: DataFrame = FileUtils.readDataFrame(spark, FileNames.appsFile)
    val userReviewsDataFrame: DataFrame = FileUtils.readDataFrame(spark, FileNames.userReviewsFile)

    import spark.implicits._

    processData(spark, appsDataFrame, userReviewsDataFrame)
  }

  private def processData(spark: SparkSession, appsDataFrame: DataFrame, userReviewsDataFrame: DataFrame) = {

    val dt_1: DataFrame = averageSentimentPolarityByAppName(userReviewsDataFrame, spark)
    val dt_2: DataFrame = saveBestApps(appsDataFrame, spark)
    val dt_3: DataFrame = cleanAppDataFrame(appsDataFrame, spark)
    val dt_4: DataFrame = joinDataFrames(dt_1, dt_3, spark)
    val dt_5: DataFrame = countApps(dt_4, spark)
  }

  // Average Sentiment Polarity by App name
  def averageSentimentPolarityByAppName(userReviewsDataFrame: DataFrame, spark: SparkSession) = {

    import spark.implicits._

    userReviewsDataFrame
      .select($"App", trim($"Sentiment_Polarity").as("Sentiment_Polarity"))
      .withColumn("Sentiment_Polarity", when($"Sentiment_Polarity".equalTo("nan"), 0)
        .otherwise($"Sentiment_Polarity"))
      .na
      .fill(0, Array("Sentiment_Polarity"))
      .withColumn("Sentiment_Polarity", $"Sentiment_Polarity".cast(DoubleType))
      .filter($"Sentiment_Polarity".isNotNull)
      .groupBy($"App")
      .avg("Sentiment_Polarity")
      .withColumnRenamed("avg(Sentiment_Polarity)", "Average_Sentiment_Polarity")
  }

  // Find and save the apps with rating bigger than 4.0 and saves it in best_apps.csv
  def saveBestApps(appsDataFrame: DataFrame, spark: SparkSession) = {

    import spark.implicits._

    val dataframe = appsDataFrame
      .filter($"rating".isNotNull.and(!$"rating".isNaN))
      .filter($"rating".geq(4.0))
      .sort($"rating".desc)

    FileUtils.saveDataframeAsSingleFileCSV("best_apps", dataframe)

    dataframe
  }

  // Cleans apps dataset
  def cleanAppDataFrame(appsDataFrame: DataFrame, spark: SparkSession) = {

    import spark.implicits._

    appsDataFrame
      .sort($"Reviews".desc)
      .withColumn("NumberSize", regexp_extract($"Size", "[0-9]+", 0))
      .withColumn("SizeUnit", regexp_extract($"Size", "[a-zA-Z]+", 0))
      .withColumn("Size", convertStringToBytes($"NumberSize", $"SizeUnit"))
      .withColumn("Price", convertToEur($"Price"))
      .withColumn("Genres", mapStringToArray($"Genres", ";"))
      .groupBy($"App")
      .agg(
        collect_set($"Category").as("Categories"),
        first($"Rating").as("Rating"),
        first($"Reviews").as("Reviews"),
        first($"Size").as("Size"),
        first($"Installs").as("Installs"),
        first($"Type").as("Type"),
        first($"Price").as("Price"),
        first($"Content Rating").as("Content_Rating"),
        first($"Genres").as("Genres"),
        first($"Last Updated").as("Last_Updated"),
        first($"Current Ver").as("Current_Version"),
        first($"Android Ver").as("Minimum_Android_Version")
      )
  }

  // Joins cleaned app data with the average sentiment polarity and saves under compressed parquet file
  def joinDataFrames(dt_1: DataFrame, dt_3: DataFrame, spark: SparkSession) = {

    import spark.implicits._

    val dataFrame = dt_3
      .withColumnRenamed("App", "App1")
      .join(dt_1, $"App1".equalTo($"App"))

    FileUtils.saveParquet(dataFrame, "googleplaystore_cleaned")

    dataFrame
  }

  // Counts apps by Genre calculating the average rating and sentiment polarity
  def countApps(dt_4: DataFrame, spark: SparkSession) = {

    import spark.implicits._

    val dataFrame = dt_4.select(explode($"Genres").as("Genre"), $"App", $"Rating", $"Average_Sentiment_Polarity")
      .groupBy("Genre")
      .agg(
        count($"App").as("Count"),
        avg($"Rating").as("Average_Rating"),
        avg($"Average_Sentiment_Polarity").as("Average_Sentiment_Polarity")
      )

    FileUtils.saveParquet(dataFrame, "googleplaystore_metrics")

    dataFrame
  }
}
