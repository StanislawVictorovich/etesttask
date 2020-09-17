package spark.testtask.snapshot

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import spark.testtask.JdbcConfig._

object SnapshotJdbcExporter extends LazyLogging {

  private def explodeMap(df: DataFrame, explodeColName: String, keyColName: String, valueColName: String = "seen") =
    df.select(col("dmpId"), explode(col(explodeColName)))
      .withColumnRenamed("key", keyColName)
      .withColumnRenamed("value", valueColName)

  private def joinSeenWithTime(df: DataFrame, seenCol: String, seenTimeCol: String, keyColName: String) = {

    val visitsTable = explodeMap(df, seenCol, keyColName)
    val seenTable = explodeMap(df, seenTimeCol, keyColName, "seenTime")

    visitsTable.
      join(seenTable, visitsTable("dmpId") === seenTable("dmpId") && visitsTable(keyColName) === seenTable(keyColName))
      .drop(seenTable("dmpId"))
      .drop(seenTable(keyColName))
  }

  def writeJdbc(table: DataFrame, name: String) =  table
    .write
    .mode(SaveMode.Overwrite)
    .jdbc(JDBC_URL, name, props)

  def main(args: Array[String]) {
    val parquetInputPath = if (args.length < 1) "snapshots_merged.parquet" else args(0)

    logger.info(s"parquet input read from: $parquetInputPath")
    val spark = SparkSession.builder.appName("Spark Test App: snapshot write to JDBC").getOrCreate()

    val snapshots = spark.read.parquet(parquetInputPath).cache

    val countriesFullTable = joinSeenWithTime(snapshots, "countries", "countrySeenTime", "country")
    val citiesFullTable = joinSeenWithTime(snapshots, "cities", "citySeenTime", keyColName = "city")
    val keywordsFullTable = joinSeenWithTime(snapshots, "keywords", "keywordSeenTime", keyColName = "keyword")
    val sitesFullTable = joinSeenWithTime(snapshots, "siteIds", "siteSeenTime", keyColName = "siteId")

    val snapshotsTable = snapshots.select("dmpId", "pageViews", "firstSeen", "lastSeen")

    val gendersTable = explodeMap(snapshots, "genders", "gender")
    val yobsTable = explodeMap(snapshots, "yobs", "yob")

    logger.info(s"yobs columns: ${yobsTable.columns}")

    writeJdbc(countriesFullTable, "country_stat")
    writeJdbc(citiesFullTable, "city_stat")
    writeJdbc(keywordsFullTable, "keyword_stat")
    writeJdbc(sitesFullTable, "site_stat")
    writeJdbc(snapshotsTable, "snapshot_stat")
    writeJdbc(gendersTable, "gender_stat")
    writeJdbc(yobsTable, "yob_stat")

    spark.stop()
  }

}
