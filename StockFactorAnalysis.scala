import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.time.LocalDate
import java.sql.Date
import scala.collection.mutable.ArrayBuffer
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
import org.apache.commons.math3.stat.correlation.Covariance
import org.apache.commons.math3.distribution.MultivariateNormalDistribution
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.sql.Dataset

object StockFactorAnalysis {
  // Create SparkSession at the object level
  val spark: SparkSession = SparkSession.builder()
    .appName("StockFactorAnalysis")
    .master("local[*]") // Use local mode for testing, remove for cluster
    .getOrCreate()
  
  // Import implicits from the spark session
  import spark.implicits._

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

  def instrumentTrialReturn(instrument: Array[Double], trial: Array[Double]): Double = {
      var instrumentTrialReturn = instrument(0)
      var i = 0
      while (i < trial.length) {
          instrumentTrialReturn += trial(i) * instrument(i+1)
          i += 1
      }
      instrumentTrialReturn
  }

  def trialReturn(trial: Array[Double], instruments: Seq[Array[Double]]): Double = {
      var totalReturn = 0.0
      for (instrument <- instruments) {
          totalReturn += instrumentTrialReturn(instrument, trial)
      }
      totalReturn / instruments.size
  }

  def trialReturns(seed: Long, numTrials: Int, factorWeights: Seq[Array[Double]], factorMeans: Array[Double], factorCovariances: Array[Array[Double]]): Seq[Double] = {
      val rand = new MersenneTwister(seed)
      val multivariateNormal = new MultivariateNormalDistribution(rand, factorMeans, factorCovariances)
      val trialReturns = new Array[Double](numTrials)
      for (i <- 0 until numTrials) {
          val trialFactorReturns = multivariateNormal.sample()
          val trialFeatures = featurize(trialFactorReturns)
          trialReturns(i) = trialReturn(trialFeatures, factorWeights)
      }
      trialReturns
  }

  def fivePercentVaR(trials: Dataset[Double]): Double = {
      val quantiles = trials.stat.approxQuantile("value",Array(0.05), 0.0)
      quantiles.head
  }

  // Main function to execute the application
  def main(args: Array[String]): Unit = {
    try {
      // Allow configuration via command-line arguments
      val start = if (args.length > 0) LocalDate.parse(args(0)) else LocalDate.parse("2009-12-01")
      val end = if (args.length > 1) LocalDate.parse(args(1)) else LocalDate.parse("2024-11-30")
      
      val directoryPath = if (args.length > 2) args(2) else "/user/sb9509_nyu_edu/stocks"
      val factorDirectoryPath = if (args.length > 3) args(3) else "/user/sb9509_nyu_edu/factors/factors"

      // Use spark.sparkContext instead of a separate SparkContext
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val stockFiles = fs.listStatus(new Path(directoryPath))
        .filter(_.getPath.getName.endsWith(".csv"))
        .map(_.getPath.toString).take(500)

      // Reading stock data files and processing
      val stockData = stockFiles.map { filePath =>
        val stockName = filePath.split("/").last.stripSuffix(".L.csv")
        val rawDf = spark.read
          .option("header", false)
          .option("inferSchema", "true")
          .csv(filePath)
        
        val filteredRDD = rawDf.rdd.zipWithIndex()
          .filter { case (_, idx) => idx >= 3 }
          .map(_._1)
        
        val filteredDf = spark.createDataFrame(filteredRDD, rawDf.schema)
        val columnNames = Seq("Date", "AdjClose", "Close", "Open", "High", "Low", "Volume")
        val finalDf = filteredDf.toDF(columnNames: _*)
        
        val selectedDf = finalDf.select($"Date", $"Close")
          .withColumn("Date", to_date($"Date", "yyyy-MM-dd"))
          .withColumn("Close", $"Close".cast("double"))
        
        val stockArray = selectedDf.as[(String, Double)].collect()
        val trimmedStockData = trimToRegion(
          stockArray.map { case (dateStr, close) => 
            (LocalDate.parse(dateStr), close) 
          }, 
          start, 
          end
        )
        
        val filledStockData = fillInHistory(trimmedStockData, start, end)
        val filledStockDf = spark.createDataFrame(filledStockData).toDF("Date", "Close")
        (stockName, filledStockDf)
      }

      // Reading factor data files and processing
      val factorFiles = fs.listStatus(new Path(factorDirectoryPath))
        .filter(_.getPath.getName.endsWith(".csv"))
        .map(_.getPath.toString)

      val factorData = factorFiles.map { filePath =>
        val factorName = filePath.split("/").last.stripSuffix(".csv")
        val rawFactorDf = spark.read
          .option("header", false)
          .option("inferSchema", "true")
          .csv(filePath)
        
        val filteredFactorRDD = rawFactorDf.rdd.zipWithIndex()
          .filter { case (_, idx) => idx >= 3 }
          .map(_._1)
        
        val filteredFactorDf = spark.createDataFrame(filteredFactorRDD, rawFactorDf.schema)
        val factorColumnNames = Seq("Date", "AdjClose", "Close", "Open", "High", "Low", "Volume")
        val finalFactorDf = filteredFactorDf.toDF(factorColumnNames: _*)
        
        val selectedFactorDf = finalFactorDf.select($"Date", $"Close")
          .withColumn("Date", to_date($"Date", "yyyy-MM-dd"))
          .withColumn("Close", $"Close".cast("double"))
        
        val factorArray = selectedFactorDf.as[(String, Double)].collect()
        val trimmedData = trimToRegion(
          factorArray.map { case (dateStr, close) => 
            (LocalDate.parse(dateStr), close) 
          }, 
          start, 
          end
        )
        
        val filledData = fillInHistory(trimmedData, start, end)
        val filledDf = spark.createDataFrame(filledData).toDF("Date", "Close")
        (factorName, filledDf)
      }

      val factorsWithReturns = factorData.map { case (factorName, filledData) =>
        val filledArray = dfToArray(filledData)
        val factorReturns = twoWeekReturns(filledArray)
        val factorReturnsData = filledArray.zip(factorReturns).map {
          case ((date, close), returns) => (date, returns)
        }
        val factorReturnsDf = spark.createDataFrame(factorReturnsData).toDF("Date", "TwoWeekReturn")
        (factorName, factorReturnsDf)
      }

      val factorReturns: Seq[Array[Double]] = factorsWithReturns.map {
        case (_, df) => 
          df.as[(LocalDate, Double)].collect().map(_._2) // Extract only the Double values
      }.toSeq

      val stockWithReturns = stockData.map { case (stockName, filledData) =>
        val filledArray = dfToArray(filledData)
        val stockReturns = twoWeekReturns(filledArray)
        val stockReturnsData = filledArray.zip(stockReturns).map {
          case ((date, close), returns) => (date, returns)
        }
        val stockReturnsDf = spark.createDataFrame(stockReturnsData).toDF("Date", "TwoWeekReturn")
        (stockName, stockReturnsDf)
      }

      val stockReturns: Seq[Array[Double]] = stockWithReturns.map {
        case (_, df) => 
          df.as[(LocalDate, Double)].collect().map(_._2) // Extract only the Double values
      }.toSeq

      println("Checkpoint 1 ")
      val factorFeatures = factorMatrix(factorReturns).map(featurize)

      val factorWeights = stockReturns.map(linearModel(_, factorFeatures)).map(_.estimateRegressionParameters()).toArray
      println("Checkpoint 2 ")
      val factorCor = new PearsonsCorrelation(factorMatrix(factorReturns)).getCorrelationMatrix().getData()

      val factorCov = new Covariance(factorMatrix(factorReturns)).getCovarianceMatrix().getData()
      println("Checkpoint 3 ")
      val factorMeans = factorReturns.map(factor => factor.sum / factor.size).toArray

      val parallelism = 1000
      val baseSeed = 1496
      println("Checkpoint 4 ")
      val seeds = (baseSeed until baseSeed + parallelism)
      val seedDS = seeds.toDS().repartition(parallelism)
      println("Checkpoint 5 ")
      val numTrials = 1000000
      val trials = seedDS.flatMap(trialReturns(_, numTrials / parallelism, factorWeights.toSeq, factorMeans, factorCov))
      println("Checkpoint 6 ")
      trials.cache()
      println("Checkpoint 7 ")
      val valueAtRisk = fivePercentVaR(trials)
      println(valueAtRisk)
      println("Checkpoint 8 ")

    } catch {
      case e: Exception => 
        println(s"An error occurred: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      // Always stop the Spark session
      spark.stop()
    }
  }
}
