package spark.testtask.snapshot.analyzer

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import spark.testtask.JdbcConfig._
import spark.testtask.TimeUtils._

object SnapshotAnalyzer extends SnapshotQueries with LazyLogging {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Spark Test App: snapshot analyzer").getOrCreate()

    val countryStat = spark.read.jdbc(JDBC_URL, "country_stat", props)
    val genderStat = spark.read.jdbc(JDBC_URL, "gender_stat", props)
    val keywordStat = spark.read.jdbc(JDBC_URL, "keyword_stat", props)
    val siteStat = spark.read.jdbc(JDBC_URL, "site_stat", props)
    val snapshotStat = spark.read.jdbc(JDBC_URL, "site_stat", props)

    val currentDays = currentTimeDays
    val lastMonth = currentDays - 30
    val last14Days = currentDays - 14

    val query1Count = countSiteVisitsByCountryAndGender(countryStat, genderStat, siteStat, "LT", 2, 37, lastMonth)
    logger.info(s"unique users we have who are from Lithuania are males and have visited siteId=37 in last month: $query1Count")

    logger.info("Top 10 visited sites for the last month:")
    topVisitedSites(siteStat, 10, lastMonth).foreach(println)

    logger.info("Top 10 most used keywords for the last month:")
    topKeywordsUsed(keywordStat, 10, lastMonth).foreach(println)

    val uniqueUsersNotVisitedSiteCount = uniqueUsersNotVisitedSiteId(snapshotStat, siteStat, 13, last14Days)

    logger.info(s"Unique users did not visit siteId=13 for the last 14 days: $uniqueUsersNotVisitedSiteCount")

    spark.stop()
  }
}
