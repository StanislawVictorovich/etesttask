package spark.testtask.visitlog

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{SaveMode, SparkSession}
import spark.testtask.TimeUtils
import spark.testtask.snapshot.Snapshot

object SnapshotBuilder extends LazyLogging {

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Spark Test App: snapshot builder").getOrCreate()
    import spark.implicits._

    val utcDays = TimeUtils.currentTimeDays
    val visitLogDS = spark.read.json(s"./logs/${utcDays}/*").as[VisitLogFromJson]

    val logs = visitLogDS.cache()

    val users = logs.groupByKey(_.dmpId)

    val snapshots = users
      .agg(VisitLogAggregator.toColumn.name("snapshot"))
      .select("snapshot.*").as[Snapshot]
      .cache()

    logger.info(s"number of snapshots: ${snapshots.count}")

    snapshots
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .parquet("snapshots.parquet")

    spark.stop()
  }
}