package spark.testtask.visitlog.generator

import java.io.{BufferedOutputStream, File, FileOutputStream, PrintWriter}
import java.util.Locale

import com.typesafe.scalalogging.LazyLogging
import org.json4s.jackson.Serialization.write
import GeoData.cities
import spark.testtask.TimeUtils
import spark.testtask.visitlog.VisitLog

import scala.util.Random

object VisitLogGenerator extends LazyLogging {

  private implicit val formats = org.json4s.DefaultFormats
  private val random = new Random()
  private val countries = Locale.getISOCountries.toSeq

  private val keywords = 1 to 1000

  private def randomCountry() = countries(random.nextInt(countries.size))

  private def randomCity(country: String) = cities
    .get(country)
    .map(list => list(random.nextInt(list.size)))

  private def randomIntOpt(max: Int) = {
    val r = random.nextInt(max)
    if (r > 0) Some(r) else None
  }

  private def randomGender() = randomIntOpt(3)

  private def randomKeyword() = keywords(random.nextInt(keywords.size))

  private def randomSiteId() = randomIntOpt(25000)

  private def randomKeywordsList(maxSize: Int) = {
    val listSize = random.nextInt(maxSize)
    if (listSize > 0) Some((for (k <- 1 to listSize) yield randomKeyword()).toList) else None
  }

  private def randomYob() = randomIntOpt(200)

  private def batchOrdered(minUserId: Int, batchSize: Int, utcDays: Int) = for {
    userId  <- minUserId to minUserId + batchSize
    visitSize = ((userId + utcDays) & 1) * 10 + random.nextInt(5) + 1
    logCount <- 1 to visitSize
    countryRand = randomCountry()
    cityRand = randomCity(countryRand)
    genderRand = randomGender()
  }  yield VisitLog(
      dmpId = s"dmp$userId",
      country = countryRand,
      city = cityRand,
      gender = genderRand,
      yob = randomYob(),
      keywords = randomKeywordsList(logCount),
      siteId = randomSiteId(),
      utcDays = utcDays
    )

  def generateVisits(logNumber: Int, minUserId: Int = 0, maxUserId: Int, logPath: String = "logs",
    utcDays: Int = TimeUtils.currentTimeDays): Unit = {
    val batchSize = 5000
    val filename = s"test_visits_log_$logNumber.json"
    val file = new File(s"$logPath/${utcDays}", filename)
    file.getParentFile.mkdirs
    val pw = new PrintWriter(new BufferedOutputStream( new FileOutputStream(file) ))

    try {
      (minUserId + 1 until maxUserId).grouped(batchSize).foreach { bt =>
        val batch = random.shuffle(batchOrdered(bt.head, batchSize, utcDays))
        for (l <- batch) {
          pw.println(write(l))
        }
        logger.info(s"generated logs batch starting from user id: ${bt.head}")
      }
    } finally {
      pw.close
      logger.info(s"generated json log file: $filename")
    }
  }

  /**
   * Main Generates args[0] number of json files with random data
   * If called without args - default number of files is 5: 100K unique dmpIds are generated
   * default path is 'logs' in the current dir
   * Second optional arg is path to output files. Generated files are put
   * to
   * $outputPath/dayNumber/test_visits_log_n.json
   *
   * where dayNumber is number of the current day like in utcDays
   * n is from 1 to number of files
   * Each json file contains records for 20K unique users/dmpIds
   *
   */
  def main(args: Array[String]): Unit = {
    val usersPerFile = 20000
    val numberOfFiles = if(args.nonEmpty) args(0).toInt else 5
    val logPath = if(args.size > 1) args(1) else "logs"
    for (i <- 1 to numberOfFiles) {
      val minUserId = (i - 1) * usersPerFile
      generateVisits(i, minUserId = minUserId, maxUserId = minUserId + usersPerFile, logPath)
    }

  }
}
