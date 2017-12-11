package com.gmail.hancury.sparkstudy

/**
  * Created by cury on 2017-12-11.
  */

object SimpleApp {
  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder().appName("SimpleApp").master("local").getOrCreate()
    val sparkContext = spark.sparkContext

    // 1. read.csv
    spark.read.format("csv").option("header","true").load("data.csv").show

    // 2. DataFrame with Schema
    import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
    import org.apache.spark.sql.Row

    val schema = StructType(StructField("k", StringType, true) :: StructField("v", IntegerType, false) :: Nil)
    spark.createDataFrame(sparkContext.emptyRDD[Row], schema).printSchema

    // 3. rollup & cube
    import spark.implicits._

    val sales = Seq(
      ("Warsaw", 2016, 100),
      ("Warsaw", 2017, 200),
      ("Boston", 2015, 50),
      ("Boston", 2016, 150),
      ("Toronto", 2017, 50)
    ).toDF("city", "year", "amount")

    sales.cube("city","year").count.show
    sales.rollup("city","year").count.show
  }
}
