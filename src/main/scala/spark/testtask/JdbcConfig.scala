package spark.testtask

import java.util.Properties

object JdbcConfig {
  val JDBC_URL = "jdbc:mysql://172.17.0.2:3306/snapshot_stat"

  val props = new Properties
  props.put("user", "root")
  props.put("password", "secret")
  props.put("rewriteBatchedStatements", "true")
  props.put("truncate", "true")
}
