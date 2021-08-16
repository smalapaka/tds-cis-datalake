package tds.core

import org.apache.spark.sql.SparkSession
import tds.common.AbstractSparkJob

object CISJsonToParquetJob extends AbstractSparkJob {
       override def init(spark: SparkSession): Unit = {
         super.init(spark)
         setJobConfigParameters(spark,tds_conf + "CISJsonToParquetJob.conf")
       }
       def process(spark: SparkSession, runMode: String): Unit = {
           val broadcastMap = spark.sparkContext.broadcast(stlMap).value
           // read json files
           val apsoDf = spark.read.json("s3://tds-dev-data/cis-data/apso_case-load*.json")
           apsoDf.write.mode("overWrite").parquet(tds_lake + "cis-apso-consolidate")
       }
       def main(args: Array[String]): Unit = {
           val spark = SparkSession.builder().appName("TDS-CISJsonToParquetJob").getOrCreate()
           init(spark)
           process(spark)
           spark.stop()
       }

}
