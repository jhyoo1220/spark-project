package jhyoo1220.app

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

object UploadTable {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sparkSession = new SparkSession.Builder()
      .config(sparkConf)
      .enableHiveSupport()
    val spark: SparkSession = sparkSession.getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val databaseToLoad: String = args(0)
    val tableToLoad: String = args(1)
    val numPartitions: Int = args(2).toInt
    val url: String = args(3)
    val dbType: String = args(4)
    val user: String = args(5)
    val password: String = args(6)
    val schemaToSave: String = args(7)
    val databaseToSave: String = args(8)
    val tableToSave: String = args(9)

    val tableToLoadFullName = s"${databaseToLoad}.${tableToLoad}"
    val tableDS = spark.sql(s"SELECT * FROM ${tableToLoadFullName}").repartition(numPartitions)
    val writer = tableDS.write.mode("overwrite")

    // postgresql is not tested
    val urlWithDatabase = dbType match {
      case "postgresql" => s"${url}/${schemaToSave}/${databaseToSave}"
      case "mysql" => s"${url}/${databaseToSave}"
      case _ => throw new Exception(s"${dbType} is currently not supported!")
    }

    val driver = dbType match {
      case "postgresql" => "org.postgresql.Driver"
      case "mysql" => "com.mysql.cj.jdbc.Driver"
      case _ => throw new Exception(s"${dbType} is currently not supported!")
    }

    val properties = new Properties()
    properties.setProperty(JDBCOptions.JDBC_DRIVER_CLASS, driver)
    properties.setProperty("user", user)
    properties.setProperty("password", password)
    properties.setProperty("useCursorFetch", "true")
    properties.setProperty("useCompression", "true")
    properties.setProperty("socketTimeout", "600000")

    writer.jdbc(urlWithDatabase, tableToSave, properties)
  }
}
