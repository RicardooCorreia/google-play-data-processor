import java.io.File

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

object FileUtils {

  val relativeDataFolder = "data/__default__/GooglePlay"

  val originalFolder = "original"

  val resultsFolder = "results"

  val filePrefix = "file://"

  def saveDataframeAsSingleFileCSV(fileName: String, data: Dataset[Row]) = {

    val projectFolder = getProjectRoot

    val fullPath = s"$projectFolder/$relativeDataFolder/$resultsFolder/$fileName"
    val fullPathWithPrefix = s"$filePrefix/$fullPath"

    val tempDataFile = s"$fullPath/temp"
    val tempDataFileWithPrefix = s"$fullPathWithPrefix/temp"

    data
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("delimiter", "§")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(tempDataFileWithPrefix)

    val folder = new File(tempDataFile)
    val filesInFolder: Array[File] = folder.listFiles()
    val outputFile = filesInFolder.filter(filterFiles).head
    outputFile.renameTo(new File(s"$fullPath.csv"))

    // Delete files generated
    filesInFolder.foreach(file => file.delete())
    // Delete temp folder
    folder.delete()
    // Delete folder with same name as csv
    new File(fullPath).delete()
  }

  def readDataFrame(spark: SparkSession, filePath: String): DataFrame = {

    val fullPath = s"$filePrefix/$getProjectRoot/$relativeDataFolder/$originalFolder/$filePath"
    val dataFrame = spark.read
      .option("header", "true")
      .csv(fullPath)
    dataFrame
  }

  def saveParquet(dataFrame: DataFrame, folderName: String) = {

    val fullPath = s"$filePrefix/$getProjectRoot/$relativeDataFolder/$resultsFolder/$folderName"
    dataFrame
      .write
      .option("compression", "gzip")
      .mode(SaveMode.Overwrite)
      .parquet(fullPath)
  }

  private def getProjectRoot = {
    System.getProperty("user.dir")
  }

  private def filterFiles(dir: File) = {
    dir.getName.startsWith("part-0000")
  }
}
