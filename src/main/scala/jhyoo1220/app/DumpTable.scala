package jhyoo1220.app

import org.apache.spark.{Partition, SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

object DumpTable {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sparkSession = new SparkSession.Builder()
      .config(sparkConf)
      .enableHiveSupport()
    val spark: SparkSession = sparkSession.getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val url: String = args(0)
    val dbType: String = args(1)
    val user: String = args(2)
    val password: String = args(3)
    val schemaToLoad: String = args(4)
    val databaseToLoad: String = args(5)
    val tableToLoad: String = args(6)
    val partitionColumn: String = args(7)
    val numPartitionsToLoad: Int = args(8).toInt
    val databaseToSave: String = args(9)
    val tableToSave: String = args(10)
    val numPartitionsToSave: Int = args(11).toInt

    val driver = dbType match {
      case "postgresql" => "org.postgresql.Driver"
      case "mysql" => "com.mysql.cj.jdbc.Driver"
      case _ => throw new Exception(s"${dbType} is currently not supported!")
    }

    val tableFullName = dbType match {
      case "postgresql" => s"${schemaToLoad}.${databaseToLoad}.${tableToLoad}"
      case "mysql" => s"${databaseToLoad}.${tableToLoad}"
    }

    val jdbcOptions = CaseInsensitiveMap[String](
      Map(
        JDBCOptions.JDBC_URL -> url,
        JDBCOptions.JDBC_DRIVER_CLASS -> driver,
        "user" -> user,
        "password" -> password,
        "useCursorFetch" -> "true",
        "useCompression" -> "true",
        "socketTimeout" -> "600000",
      ))

    val partitionAddedOptions =
      if (numPartitionsToLoad > 1) {
        val partitionValues = spark
          .read
          .format("jdbc")
          .options(jdbcOptions)
          .option(JDBCOptions.JDBC_QUERY_STRING, s"SELECT MIN(${partitionColumn}), MAX(${partitionColumn}) FROM ${tableFullName}")
          .load
          .first

        val partitionOptions = CaseInsensitiveMap(
          Map(
            JDBCOptions.JDBC_LOWER_BOUND -> partitionValues.getAs[Int](0).toString,
            JDBCOptions.JDBC_UPPER_BOUND -> partitionValues.getAs[Int](1).toString,
            JDBCOptions.JDBC_NUM_PARTITIONS -> numPartitionsToLoad.toString,
            JDBCOptions.JDBC_PARTITION_COLUMN -> partitionColumn))

        jdbcOptions ++ partitionOptions
      } else {
        jdbcOptions
      }

    val tableDS = spark
      .read
      .format("jdbc")
      .options(partitionAddedOptions)
      .option(JDBCOptions.JDBC_TABLE_NAME, tableFullName)
      .load

    tableDS
      .repartition(numPartitionsToSave)
      .write
      .mode("overwrite")
      .format("orc")
      .option("compression", "zlib")
      .saveAsTable(s"${databaseToSave}.${tableToSave}")
  }
}
