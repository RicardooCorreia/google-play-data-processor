package spark

import org.apache.spark.sql.SparkSession

object SparkSessionProvider {

  def getSparkSession(appName: String): SparkSession = {

    SparkSession.builder()
      .appName(appName)
      .config("spark.master", "local")
      .getOrCreate()
  }
}
