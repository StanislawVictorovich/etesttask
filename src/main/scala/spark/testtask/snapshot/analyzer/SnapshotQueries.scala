package spark.testtask.snapshot.analyzer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, first, sum}

trait SnapshotQueries {

  def countSiteVisitsByCountryAndGender(countryStat: DataFrame, genderStat:DataFrame, siteStat:DataFrame, country: String, gender: Int, siteId: Int, minDays: Int) = {
    val fromCountry = countryStat.where(col("country") === country)
    val withGender = genderStat.where(col("gender") === gender)
    val visitedSiteId = siteStat.where(col("siteId") === siteId and col("seenTime") >= minDays)
    fromCountry
      .join(withGender, fromCountry("dmpId") === withGender("dmpId"))
      .drop(withGender("dmpId"))
      .join(visitedSiteId, visitedSiteId("dmpId") === fromCountry("dmpId"))
      .count
  }

  def topVisitedSites(siteStat: DataFrame, n: Int, minDays: Int) = siteStat
    .where(col("seenTime") >= minDays)
    .groupBy(col("siteId"))
    .agg(sum(col("seen")).as("seen"))
    .orderBy(col("seen").desc)
    .take(n)

  def topKeywordsUsed(keywordStat: DataFrame, n: Int, minDays: Int) = keywordStat
    .where(col("seenTime") >= minDays)
    .groupBy(col("keyword"))
    .agg(sum(col("seen")).as("seen"))
    .orderBy(col("seen").desc)
      .take(n)

  def uniqueUsersNotVisitedSiteId(snapshotStat: DataFrame, siteStat: DataFrame, siteId: Int, minDays: Int): Long =
    snapshotStat
    .where(col("seenTime") >= minDays)
    .join(siteStat, snapshotStat("dmpId") === siteStat("dmpId") and siteStat("siteId") === siteId, joinType = "LEFT")
    .where(siteStat("dmpId") isNull)
    .drop(siteStat("dmpId"))
    .groupBy("dmpId").agg(first(col("dmpId")))
    .count
}
