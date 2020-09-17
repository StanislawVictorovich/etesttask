package spark.testtask.snapshot

case class Snapshot(
  dmpId: String,
  countries: Map[String, Int], // a map of country -> seen count (record count)
  countrySeenTime: Map[String, Int], // a map of country -> last seen time of country
  cities: Option[Map[String, Int]], // an optional map of city -> seen count
  citySeenTime: Option[Map[String, Int]], // an optional map of city -> last seen time
  genders: Option[Map[Int, Int]], // seen count map
  yobs: Option[Map[Int, Int]], // seen count map
  keywords: Map[Int, Int], // keyword -> seen time count
  keywordSeenTime: Map[Int, Int], // keyword -> last seen time
  siteIds: Option[Map[Int, Int]], // site -> seen count
  siteSeenTime: Option[Map[Int, Int]], // site -> last seen time
  pageViews: Long, // lifetime pageviews
  firstSeen: Int, // historically first seen time
  lastSeen: Int // last seen time
) {
  def merge(other: Snapshot) = Snapshot(
    dmpId = other.dmpId, // must be the same as other.dmpId
    countries = this.countries ++ other.countries,
    countrySeenTime = this.countrySeenTime ++ other.countrySeenTime,
    cities = sumMapOpts(this.cities, other.cities),
    citySeenTime = sumMapOpts(this.citySeenTime, other.citySeenTime),
    genders = sumMapOpts(this.genders, other.genders),
    yobs = sumMapOpts(this.yobs, other.yobs),
    keywords = this.keywords ++ other.keywords,
    keywordSeenTime = this.keywordSeenTime ++ other.keywordSeenTime,
    siteIds = sumMapOpts(this.siteIds, other.siteIds),
    siteSeenTime = sumMapOpts(this.siteSeenTime, other.siteSeenTime),
    pageViews = this.pageViews + other.pageViews,
    firstSeen = Math.min(this.firstSeen, other.firstSeen),
    lastSeen = Math.max(lastSeen, other.lastSeen)
  )

  private def sumMapOpts[K, V](m1:Option[Map[K, V]], m2: Option[Map[K, V]]) = (m1, m2) match {
    case (Some(map1), Some(map2)) => Some(map1 ++ map2)
    case (Some(map), None) => Some(map)
    case (None, Some(map)) => Some(map)
    case _ => None
  }
}

object Snapshot {
  def empty = Snapshot(
    dmpId = "",
    countries = Map.empty,
    countrySeenTime = Map.empty,
    cities = None,
    citySeenTime = None,
    genders = None,
    yobs = None,
    keywords = Map.empty,
    keywordSeenTime = Map.empty,
    siteIds = None,
    siteSeenTime = None,
    pageViews = 0L,
    firstSeen = Int.MaxValue,
    lastSeen = 0
  )
}