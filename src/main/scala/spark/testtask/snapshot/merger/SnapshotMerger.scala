package spark.testtask.snapshot.merger

import org.apache.spark.sql.{SaveMode, SparkSession}
import spark.testtask.snapshot.Snapshot

object SnapshotMerger {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Spark Test App: snapshot merger").getOrCreate()
    import spark.implicits._
    val snapshots = spark.read.parquet("snapshots.parquet").as[Snapshot].cache

    val grouped = snapshots.groupByKey(_.dmpId)

    val snapshotsMerged = grouped
      .agg(SnapshotAggregator.toColumn.name("aggregated"))
      .select("aggregated.*").as[Snapshot]
      .cache()

    snapshotsMerged
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .parquet("snapshots_merged.parquet")
  }

}
