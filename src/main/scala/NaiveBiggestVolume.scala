import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, SQLContext, SparkSession}

object NaiveBiggestVolume {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .appName("NaiveBiggestVolume")
      .config(conf)
      .getOrCreate()

    val xetra_df = spark.sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/data-set/*/*.csv")
      .createOrReplaceTempView("xetra")

    val sqlQuery =
      """select SecurityDesc, SecurityID, avg((MaxPrice - MinPrice) / MinPrice) as implied_volume from xetra
        | group by SecurityDesc, SecurityID
        | order by implied_volume desc""".stripMargin

    val resultsDF = spark.sql(sqlQuery)
    resultsDF.show(10)

    System.in.read
    spark.stop
  }

}