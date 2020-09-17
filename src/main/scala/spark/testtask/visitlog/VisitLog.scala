package spark.testtask.visitlog

import spark.testtask.TimeUtils

case class VisitLog(
  dmpId: String,
  country: String,
  city: Option[String] = None,
  gender: Option[Int] = None,
  yob: Option[Int] = None,
  keywords: Option[List[Int]] = None,
  siteId: Option[Int] = None,
  utcDays: Int = TimeUtils.currentTimeDays
)

case class VisitLogFromJson(
  dmpId: String,
  country: String,
  city: Option[String] = None,
  gender: Option[Long] = None,
  yob: Option[Long] = None,
  keywords: Option[List[Long]] = None,
  siteId: Option[Long] = None,
  utcDays: Long
)