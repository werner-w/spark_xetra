import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}


object BiggestWinner {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .appName("BiggestWinner")
      .config(conf)
      .getOrCreate()

    spark.sqlContext.setConf("spark.sql.caseSensitive", "false")


    val df_xetra = spark.sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .load("src/main/resources/data-set/*/*.csv")
      .createOrReplaceTempView("xetra")

    val query =
      """
        |select allreturns.Date, allreturns.SecurityID, allreturns.SecurityDesc, allreturns.percentchange
        |
        |from (
        |
        |    select xetra1.Date, max((xetra2.EndPrice - xetra1.StartPrice) / xetra1.StartPrice) as percentchange
        |
        |    from (
        |
        |        select SecurityID, min(Time) as mintime, max(Time) as maxtime, Date from xetra
        |        group by SecurityID, Date
        |    ) as timestamps
        |
        |    left join xetra as xetra1
        |    on xetra1.SecurityID = timestamps.SecurityID and xetra1.Date = timestamps.Date and xetra1.Time = timestamps.mintime
        |
        |    left join xetra as xetra2
        |    on xetra2.SecurityID = timestamps.SecurityID and xetra2.Date = timestamps.Date and xetra2.Time = timestamps.maxtime
        |
        |    group by xetra1.Date
        |) as maxreturns
        |
        |left join (
        |    select xetra1.SecurityID, xetra1.SecurityDesc, xetra1.Date, (xetra2.EndPrice - xetra1.StartPrice) / xetra1.StartPrice as percentchange
        |
        |    from (
        |
        |        select SecurityID, min(Time) as mintime, max(Time) as maxtime, Date from xetra
        |        group by SecurityID, Date
        |    ) as timestamps
        |
        |    left join xetra as xetra1
        |    on xetra1.SecurityID = timestamps.SecurityID and xetra1.Date = timestamps.Date and xetra1.Time = timestamps.mintime
        |
        |    left join xetra as xetra2
        |    on xetra2.SecurityID = timestamps.SecurityID and xetra2.Date = timestamps.Date and xetra2.Time = timestamps.maxtime
        |) as allreturns
        |
        |on maxreturns.percentchange = allreturns.percentchange and maxreturns.Date = allreturns.Date
        |
        |order by allreturns.Date
      """.stripMargin

    val res = spark.sql(query)
    res.show(10)

    System.in.read
    spark.stop
  }

}
