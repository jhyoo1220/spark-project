package jhyoo1220.app

import java.util.UUID

import jhyoo1220.model.{EnglishName, User}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.typedLit

object GenerateRandomUsers {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sparkSession = new SparkSession.Builder()
      .config(sparkConf)
      .enableHiveSupport()
    val spark: SparkSession = sparkSession.getOrCreate()
    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    val numUsers: Int = args(0).toInt
    val database: String = args(1)
    val table: String = args(2)
    val partitionColumn: String = args(3)
    val partitionDate: String = args(4)

    val users = (1 to numUsers)
      .map { id =>
        val uuid = UUID.randomUUID.toString
        val name = EnglishName.getName

        User(id, uuid, name)
      }

    val userRDD = spark.sparkContext.parallelize(users)
    val userDS = spark
      .createDataset(userRDD)
      .withColumn(partitionColumn, typedLit[String](partitionDate))

    val writer = userDS.write
      .mode("overwrite")
      .format("orc")
      .option("compression", "zlib")

    if (spark.catalog.tableExists(database, table)) {
      writer.insertInto(s"$database.$table")
    } else {
      writer.partitionBy(partitionColumn).saveAsTable(s"$database.$table")
    }
  }
}
