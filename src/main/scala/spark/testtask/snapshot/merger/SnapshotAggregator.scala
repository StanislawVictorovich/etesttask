package spark.testtask.snapshot.merger

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}
import spark.testtask.snapshot.Snapshot

object SnapshotAggregator extends Aggregator[Snapshot, Snapshot, Snapshot] {
  def zero: Snapshot = Snapshot.empty

  def reduce(snapshotAggr: Snapshot, snapshot: Snapshot): Snapshot = snapshotAggr.merge(snapshot)

  def merge(s1: Snapshot, s2: Snapshot): Snapshot = s1 merge s2

  def finish(s: Snapshot): Snapshot = s

  def bufferEncoder: Encoder[Snapshot] = Encoders.product

  def outputEncoder: Encoder[Snapshot] = Encoders.product
}
