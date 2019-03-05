import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}


object MostTradedByVolume {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .appName("MostTradedByVolume")
      .config(conf)
      .getOrCreate()

    val df = spark.sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/main/resources/data-set/*/*.csv")
      .createOrReplaceTempView("xetra")

    val query =
      """
        |select sumtable.Date, sumtable.SecurityId, sumtable.SecurityDesc, maxtable.maxamount
        |
        |from (
        |    select Date, max(tradeamountmm) as maxamount
        |
        |    from (
        |
        |        select SecurityId, SecurityDesc, Date, sum(StartPrice * TradedVolume) / 1e6 as tradeamountmm
        |        from xetra
        |        where currency = 'EUR'
        |        group by SecurityId, SecurityDesc, Date
        |        order by Date asc, tradeamountmm desc
        |    )
        |
        |    group by date
        |) as maxtable
        |
        |left join (
        |    select SecurityId, SecurityDesc, Date, sum(StartPrice * TradedVolume) / 1e6 as tradeamountmm
        |    from xetra
        |    where currency = 'EUR'
        |    group by SecurityId, SecurityDesc, Date
        |    order by date asc, tradeamountmm desc
        |) as sumtable
        |
        |on sumtable.tradeamountmm = maxtable.maxamount and sumtable.Date = maxtable.Date
        |
        |order by sumtable.Date asc, maxtable.maxamount desc
      """.stripMargin

    val topLevel = spark.sql(query)
      .show(100)

    System.in.read
    spark.stop
  }

}