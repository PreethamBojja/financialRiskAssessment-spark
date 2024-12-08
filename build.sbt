name := "StockFactorAnalysis"

version := "0.1"

scalaVersion := "2.12.10"  // Ensure this matches your Scala version

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.0",  // Use the Spark version compatible with your setup
  "org.apache.spark" %% "spark-sql" % "3.4.0",   // Spark SQL for DataFrame operations
  "org.apache.hadoop" % "hadoop-common" % "3.3.1", // Required for Hadoop file system interactions
  "org.apache.commons" % "commons-math3" % "3.6.1", // Required for OLSMultipleLinearRegression
  "joda-time" % "joda-time" % "2.10.10"  // For DateTime manipulation
)

resolvers += "Spark Packages Repo" at "https://repo.spark-packages.org"

