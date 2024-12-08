import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.time.LocalDate
import java.sql.Date
import scala.collection.mutable.ArrayBuffer
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression

object StockFactorAnalysis {

  // Create a SparkSession object
  val spark: SparkSession = SparkSession.builder()
    .appName("StockFactorAnalysis")
    .master("local[*]")  // Adjust this if you're using a cluster
    .getOrCreate()

  import spark.implicits._  // Now spark.implicits._ will work

  // Define the trimToRegion function
  def trimToRegion(history: Array[(LocalDate, Double)], start: LocalDate, end: LocalDate): Array[(LocalDate, Double)] = {
    var trimmed = history.dropWhile(_._1.isBefore(start))
      .takeWhile(x => x._1.isBefore(end) || x._1.isEqual(end))

    if (trimmed.nonEmpty && trimmed.head._1 != start) {
      trimmed = Array((start, trimmed.head._2)) ++ trimmed
    }
    if (trimmed.nonEmpty && trimmed.last._1 != end) {
      trimmed = trimmed ++ Array((end, trimmed.last._2))
    }
    trimmed
  }

  // Function to fill missing history (excluding weekends)
  def fillInHistory(history: Array[(LocalDate, Double)], start: LocalDate, end: LocalDate): Array[(LocalDate, Double)] = {
    var cur = history
    val filled = new ArrayBuffer[(LocalDate, Double)]()
    var curDate = start

    while (!curDate.isAfter(end)) {
      if (cur.nonEmpty && cur.head._1.isEqual(curDate)) {
        filled += ((curDate, cur.head._2)) // Add the existing stock data
        cur = cur.tail
      } else {
        // If no data for the current date, repeat the last known value
        filled += ((curDate, cur.head._2))
      }

      // Move to the next day
      curDate = curDate.plusDays(1)

      // Skip weekends (Saturday and Sunday)
      if (curDate.getDayOfWeek == java.time.DayOfWeek.SATURDAY || curDate.getDayOfWeek == java.time.DayOfWeek.SUNDAY) {
        curDate = curDate.plusDays(2)
      }
    }

    filled.toArray
  }

  // Function to calculate two-week returns
  def twoWeekReturns(history: Array[(LocalDate, Double)]): Array[Double] = {
    history.sliding(10)
      .map { window =>
        val next = window.last._2
        val prev = window.head._2
        (next - prev) / prev
      }.toArray
  }

  // Convert DataFrame to Array[(LocalDate, Double)]
  def dfToArray(df: DataFrame): Array[(LocalDate, Double)] = {
    df.select("Date", "Close")
      .as[(String, Double)] // Temporarily select String (date) and Double (close)
      .map { case (dateStr, close) =>
        (LocalDate.parse(dateStr), close) // Convert to LocalDate
      }
      .collect()
      .toArray
  }

  // Function to create the factor matrix
  def factorMatrix(histories: Seq[Array[Double]]): Array[Array[Double]] = {
    val mat = new Array[Array[Double]](histories.head.length)
    for (i <- histories.head.indices) {
      mat(i) = histories.map(_(i)).toArray
    }
    mat
  }

  // Function to featurize the factor returns
  def featurize(factorReturns: Array[Double]): Array[Double] = {
    val squaredReturns = factorReturns.map(x => math.signum(x) * x * x)
    val squareRootedReturns = factorReturns.map(x => math.signum(x) * math.sqrt(math.abs(x)))
    squaredReturns ++ squareRootedReturns ++ factorReturns
  }

  // Function to create a linear regression model
  def linearModel(instrument: Array[Double], factorMatrix: Array[Array[Double]]): OLSMultipleLinearRegression = {
    val regression = new OLSMultipleLinearRegression()
    regression.newSampleData(instrument, factorMatrix)
    regression
  }

  // Main function to execute the application
  def main(args: Array[String]): Unit = {
    val start = LocalDate.parse("2023-12-01")
    val end = LocalDate.parse("2024-11-30")
    val directoryPath = "/user/sb9509_nyu_edu/stocks"
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val stockFiles = fs.listStatus(new Path(directoryPath)).filter(_.getPath.getName.endsWith(".csv")).map(_.getPath.toString)

    // Reading stock data files and processing
    val stockData = stockFiles.map { filePath =>
      val stockName = filePath.split("/").last.stripSuffix(".L.csv")
      val rawDf = spark.read.option("header", false).option("inferSchema", "true").csv(filePath)
      val filteredRDD = rawDf.rdd.zipWithIndex().filter { case (_, idx) => idx >= 3 }.map(_._1)
      val filteredDf = spark.createDataFrame(filteredRDD, rawDf.schema)
      val columnNames = Seq("Date", "AdjClose", "Close", "Open", "High", "Low", "Volume")
      val finalDf = filteredDf.toDF(columnNames: _*)
      val selectedDf = finalDf.select($"Date", $"Close").withColumn("Date", to_date($"Date", "yyyy-MM-dd")).withColumn("Close", $"Close".cast("double"))
      val stockArray = selectedDf.as[(String, Double)].collect()
      val trimmedStockData = trimToRegion(stockArray.map { case (dateStr, close) => (LocalDate.parse(dateStr), close) }, start, end)
      val filledStockData = fillInHistory(trimmedStockData, start, end)
      val filledStockDf = spark.createDataFrame(filledStockData).toDF("Date", "Close")
      (stockName, filledStockDf)
    }

    // Reading factor data files and processing
    val factorDirectoryPath = "/user/sb9509_nyu_edu/factors/factors"
    val factorFiles = fs.listStatus(new Path(factorDirectoryPath)).filter(_.getPath.getName.endsWith(".csv")).map(_.getPath.toString)

    val factorData = factorFiles.map { filePath =>
      val factorName = filePath.split("/").last.stripSuffix(".csv")
      val rawFactorDf = spark.read.option("header", false).option("inferSchema", "true").csv(filePath)
      val filteredFactorRDD = rawFactorDf.rdd.zipWithIndex().filter { case (_, idx) => idx >= 3 }.map(_._1)
      val filteredFactorDf = spark.createDataFrame(filteredFactorRDD, rawFactorDf.schema)
      val factorColumnNames = Seq("Date", "AdjClose", "Close", "Open", "High", "Low", "Volume")
      val finalFactorDf = filteredFactorDf.toDF(factorColumnNames: _*)
      val selectedFactorDf = finalFactorDf.select($"Date", $"Close").withColumn("Date", to_date($"Date", "yyyy-MM-dd")).withColumn("Close", $"Close".cast("double"))
      val factorArray = selectedFactorDf.as[(String, Double)].collect()
      val trimmedData = trimToRegion(factorArray.map { case (dateStr, close) => (LocalDate.parse(dateStr), close) }, start, end)
      val filledData = fillInHistory(trimmedData, start, end)
      val filledDf = spark.createDataFrame(filledData).toDF("Date", "Close")
      (factorName, filledDf)
    }

    // Calculating factor returns
    val factorReturns = factorData.map { case (factorName, filledData) =>
      val filledArray = dfToArray(filledData)
      val factorReturns = twoWeekReturns(filledArray)
      val factorReturnsData = filledArray.zip(factorReturns).map {
        case ((date, close), returns) => (date, returns)
      }
      val factorReturnsDf = spark.createDataFrame(factorReturnsData).toDF("Date", "TwoWeekReturn")
      (factorName, factorReturnsDf)
    }

    // Creating the factor feature matrix
    val factorFeatures = factorMatrix(factorReturns.map { case (_, df) =>
      df.as[(LocalDate, Double)].collect().map(_._2)
    })
      .map(featurize)

    // Calculating stock returns
    val stockReturns = stockData.map { case (stockName, filledData) =>
      val filledArray = dfToArray(filledData)
      val stockReturns = twoWeekReturns(filledArray)
      val stockReturnsData = filledArray.zip(stockReturns).map {
        case ((date, close), returns) => (date, returns)
      }
      val stockReturnsDf = spark.createDataFrame(stockReturnsData).toDF("Date", "TwoWeekReturn")
      (stockName, stockReturnsDf)
    }

    // Extracting the stock return values for regression
    val stockReturnValues = stockReturns.map { case (_, df) =>
      df.as[(LocalDate, Double)].collect().map(_._2)
    }

    // Performing linear regression for each stock
    val factorWeights = stockReturnValues.map { stockReturns =>
      linearModel(stockReturns, factorFeatures).estimateRegressionParameters()
    }.toArray

    // Save or display the factor weights
    println(s"Factor Weights: ${factorWeights.mkString(", ")}")
    spark.stop() // Stop the Spark session after use
  }
}
