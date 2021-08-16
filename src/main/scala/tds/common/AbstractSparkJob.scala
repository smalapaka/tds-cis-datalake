package tds.common

import tds.util.CommonUtils._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat, lit}
import org.apache.log4j.{Level, Logger}

abstract class AbstractSparkJob {
   var stlMap: java.util.HashMap[String, String] =_
   var jobParamMap: java.util.HashMap[String, String] =_
   var spark:SparkSession = _

   // set env variables
   val tds_lake = "s3://tds-dev-data/cis-data-lake/"
   val tds_src_data = "s3://tds-dev-data/cis-data/"
   val tds_conf = "s3://tds-dev-data/tds-conf/"

   val rootLogger = Logger.getRootLogger()
   rootLogger.setLevel(Level.ERROR)

   // Initialize spark session
   def process(spark: SparkSession, runMode: String = "FULL"): Unit

   //  Populate table location map
   def init(spark: SparkSession): Unit = {
       spark.conf.set("spark.sql.shuffle.partitions","10")
       populateTableLocationMap(spark, "s3://tds-dev-data/stl/tds-schema-table-location.txt")
   }
   //s3://tds-dev-data/stl/conf/ApsoJsonToParquetJob.conf
   // Populate Schema table location Map to get a location of a table
   def populateTableLocationMap(spark: SparkSession, fileLocation: String): Unit = {
       //import spark.sqlContext.implecits._
       import spark.sqlContext.implicits._
       val df = spark.read.option("header",true).csv(fileLocation).cache()
       val arrayRecords = df.select(concat($"SCHEMA", lit("-"), $"TABLE"), $"LOCATION").as[(String, String)].collect()
       stlMap = newMap(arrayRecords)
   }

   def setJobConfigParameters(spark: SparkSession, fileLocation: String): Unit = {
       import spark.sqlContext.implicits._

       //Read config file from S3 config folder
       val df = spark.read.option("header", true).csv(fileLocation).cache()
       //Populate Array and set parameters using spark.conf.set
       val arrayRecords = df.select($"KEY", $"VALUE").as[(String, String)].collect()

       //for ((k,v) <- arrayRecords) {
       //    spark.conf.set(k, v)
       //}
       arrayRecords.foreach {case (key,value) => spark.conf.set(key,value)}
   }
}
