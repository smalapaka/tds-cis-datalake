package tds.core

import org.apache.spark.sql.SparkSession
import tds.common.AbstractSparkJob

object TdsDataProcessJob extends AbstractSparkJob {
  override def init(spark: SparkSession): Unit = {
    super.init(spark)
    setJobConfigParameters(spark,tds_conf + "TdsDataProcessJob.conf")
  }
  def process(spark: SparkSession, runMode: String): Unit = {
    val broadcastMap = spark.sparkContext.broadcast(stlMap).value

    // read parquet
    spark.read.parquet(tds_lake + "cis-apso-consolidate").createOrReplaceTempView("CisConsolidatedTbl")

    //Create SQL
    val cisAggrSql = new StringBuilder()
    cisAggrSql ++= "select "
    cisAggrSql ++= "id, "
    cisAggrSql ++= "a_number, "
    cisAggrSql ++= """cast(unix_timestamp(admin_closed_date,"mm/dd/yyyy") as timestamp) as admin_closed_date, """
    cisAggrSql ++= "basis_of_claim, "
    cisAggrSql ++= "case_id, "
    cisAggrSql ++= "citizenship, "
    cisAggrSql ++= """cast(unix_timestamp(clock_in_date,"mm/dd/yyyy") as timestamp) as clock_in_date, """
    cisAggrSql ++= "credibility_established, "
    cisAggrSql ++= """cast(unix_timestamp(decision_date,"mm/dd/yyyy") as timestamp) as decision_date, """
    cisAggrSql ++= "detention_facility, "
    cisAggrSql ++= "fear_determination, "
    cisAggrSql ++= "fear_type, "
    cisAggrSql ++= "port_of_entry, "
    cisAggrSql ++= "receipt_number, "
    cisAggrSql ++= "special_group, "
    cisAggrSql ++= """cast(date_format(cast(unix_timestamp(clock_in_date,"mm/dd/yyyy") as timestamp),"yyyy") as int) as clock_year """
    cisAggrSql ++= "from CisConsolidatedTbl "

    val cisAggr = spark.sql(cisAggrSql.toString())

    cisAggr.write.partitionBy("clock_year").mode("overWrite").parquet(tds_lake + "cis_apso_fct")

  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TDS-TdsDataProcessJob").getOrCreate()
    init(spark)
    process(spark)
    spark.stop()
  }

}