package spark.testtask

import java.util.concurrent.TimeUnit

object TimeUtils {
  def currentTimeDays: Int = TimeUnit.MILLISECONDS.toDays(System.currentTimeMillis()).toInt
}
