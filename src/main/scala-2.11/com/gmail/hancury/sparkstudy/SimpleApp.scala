package com.gmail.hancury.sparkstudy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by cury on 2017-12-11.
  */

object SimpleApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SimpleApp").master("local").getOrCreate()
    import spark.sqlContext.implicits._

    val sales = Seq(
      ("Warsaw", 2016, 100),
      ("Warsaw", 2017, 200),
      ("Boston", 2015, 50),
      ("Boston", 2016, 150),
      ("Toronto", 2017, 50)
    ).toDF("city", "year", "amount")

    sales.printSchema()

    sales.rollup("city","year")
      .sum("amount")
      .orderBy(col("city").desc, col("year").desc)
      .show()

    sales.cube("city","year").count.show
  }
}
