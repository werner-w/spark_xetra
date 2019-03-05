import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BiggestVolume {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .appName("BiggestVolume")
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val xetra_df = spark.sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/data-set/*/*.csv")

    val fraction = udf((MaxPrice: Double, MinPrice: Double) => (MaxPrice - MinPrice)/MinPrice)

    xetra_df
      .withColumn("implied_volume", fraction($"MaxPrice", $"MinPrice"))
      .groupBy($"SecurityDesc", $"SecurityID")
      .agg(avg($"implied_volume"))
      .orderBy(desc("avg(implied_volume)"))
      .show(10)


    System.in.read
    spark.stop
  }

}