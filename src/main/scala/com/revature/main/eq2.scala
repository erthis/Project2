package com.revature.main

import com.revature.main.Project2.{saveDataFrameAsCSV, spark}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, date_format, expr, not, to_timestamp, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

import com.revature.main.Project2.{saveDataFrameAsCSV, spark}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

import scala.collection.mutable.ArrayBuffer


object eq2 {
  /**
    *Fetches the deaths per state, prints to CSV.
    *
    *
    */

  def getTotalDeathsPerState2019  (): Unit = {
    // Read "time_series_covid_19_confirmed_US.csv" data as a dataframe
    println("Reading US total deaths by state into a dataframe from CSV:")
    var startTime = System.currentTimeMillis()
    var df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("WeeklyDeaths2014-2019.csv")
    df.printSchema()
    var transTime = (System.currentTimeMillis() - startTime) / 1000d
    println(s"Table length: ${df.count()}")
    println(s"Transaction time: $transTime seconds")
    df = df.withColumnRenamed("All  Cause", "Deaths") //CDC decided to give "All Cause" two spaces.
      .withColumnRenamed("Jurisdiction of Occurrence", "Name")
      .withColumnRenamed("MMWR Year", "Year")
      .withColumnRenamed("MMWR Week", "Week")
      .withColumn("Deaths", col("Deaths").cast("double"))
    df = df.filter(df("Year") === "2019")
      .where("Name == 'Texas'")
    var clonedf = df.filter(df("Year") === "3000")
      .where("Name == 'Texas'")
    df = df.select("Deaths", "Name", "Week")
    clonedf = clonedf.select("Deaths", "Name", "Week")
    // val pivotDF = df.groupBy("Name").pivot("Week").sum("Deaths").orderBy("Name")
    // pivotDF.show(100)
    saveDataFrameAsCSV(df, "TexasStateDeathsByWeek2019NonAggregate.csv")
    df.unpersist()
    //pivotDF.unpersist()


    var df2 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("WeeklyDeaths2020.csv")
    df2.printSchema()
    println(s"Table length: ${df2.count()}")
    println(s"Transaction time: $transTime seconds")
    df2 = df2.withColumnRenamed("All Cause", "Deaths") //This one has only one space for some reason.
      .withColumnRenamed("Jurisdiction of Occurrence", "Name")
      .withColumnRenamed("MMWR Year", "Year")
      .withColumnRenamed("MMWR Week", "Week")
      .withColumn("Deaths", col("Deaths").cast("double"))
    df2 = df2.filter(df2("Year") === "2020")
      .where("Name == 'Texas'")
    df2 = df2.select("Deaths", "Name", "Week")
    val df2Schema = df2.schema
    //val pivotDF2 = df2.groupBy("Name").pivot("Week").sum("Deaths").orderBy("Name")
    // pivotDF2.show(100)
    saveDataFrameAsCSV(df2, "PivotedStateDeathsByWeek2020NonAggregate.csv")
    df2.unpersist()
    // pivotDF2.unpersist()

    var df3 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("time_series_covid_19_deaths_US.csv")
    df3 = df3.withColumnRenamed("Admin2", "County")
      .withColumnRenamed("Province_state", "Name")
      .where("Name == 'Texas'")
    df3 = df3.withColumnRenamed("1/25/20", "4Agg")
      .withColumnRenamed("2/1/20", "5Agg")
      .withColumnRenamed("2/8/20", "6Agg")
      .withColumnRenamed("2/15/20", "7Agg")
      .withColumnRenamed("2/22/20", "8Agg")
      .withColumnRenamed("2/29/20", "9Agg")
      .withColumnRenamed("3/7/20", "10Agg")
      .withColumnRenamed("3/14/20", "11Agg")
      .withColumnRenamed("3/21/20", "12Agg")
      .withColumnRenamed("3/28/20", "13Agg")
      .withColumnRenamed("4/4/20", "14Agg")
      .withColumnRenamed("4/11/20", "15Agg")
      .withColumnRenamed("4/18/20", "16Agg")
      .withColumnRenamed("4/25/20", "17Agg")
      .withColumnRenamed("5/2/20", "18Agg")
      .withColumnRenamed("5/9/20", "19Agg")
      .withColumnRenamed("5/16/20", "20Agg")
      .withColumnRenamed("5/23/20", "21Agg")
      .withColumnRenamed("5/30/20", "22Agg")
      .withColumnRenamed("6/6/20", "23Agg")
      .withColumnRenamed("6/13/20", "24Agg")
      .withColumnRenamed("6/20/20", "25Agg")
      .withColumnRenamed("6/27/20", "26Agg")
      .withColumnRenamed("7/4/20", "27Agg")
      .withColumnRenamed("7/11/20", "28Agg")
      .withColumnRenamed("7/18/20", "29Agg")
      .withColumnRenamed("7/25/20", "30Agg")
      .withColumnRenamed("8/1/20", "31Agg")
      .withColumnRenamed("8/8/20", "32Agg")
      .withColumnRenamed("8/15/20", "33Agg")
      .withColumnRenamed("8/22/20", "34Agg")
      .withColumnRenamed("8/29/20", "35Agg")
      .withColumnRenamed("9/5/20", "36Agg")
      .withColumnRenamed("9/12/20", "37Agg")
      .withColumnRenamed("9/19/20", "38Agg")
      .withColumnRenamed("9/26/20", "39Agg")
      .withColumnRenamed("10/3/20", "40Agg")
      .withColumnRenamed("10/10/20", "41Agg")
      .withColumnRenamed("10/17/20", "42Agg")
      .withColumnRenamed("10/24/20", "43Agg")
      .withColumnRenamed("10/31/20", "44Agg")
      .withColumnRenamed("11/7/20", "45Agg")
      .withColumnRenamed("11/14/20", "46Agg")
      .withColumnRenamed("11/21/20", "47Agg")
      .withColumnRenamed("11/28/20", "48Agg")
      .withColumnRenamed("12/5/20", "49Agg")
      .withColumnRenamed("12/12/20", "50Agg")
      .withColumnRenamed("12/19/20", "51Agg")
      .withColumnRenamed("12/26/20", "52Agg")

    df3 = df3.groupBy(col("Name"))
      .sum("Population", "4Agg","5Agg", "6Agg","7Agg", "8Agg","9Agg", "10Agg",
        "11Agg", "12Agg","13Agg", "14Agg","15Agg", "16Agg","17Agg", "18Agg", "19Agg", "20Agg","21Agg", "22Agg","23Agg", "24Agg",
        "25Agg", "26Agg","27Agg", "28Agg","29Agg", "30Agg","31Agg", "32Agg","33Agg", "34Agg","35Agg", "36Agg","37Agg", "38Agg",
        "39Agg", "40Agg","41Agg", "42Agg","43Agg", "44Agg","45Agg", "46Agg","47Agg", "48Agg","49Agg", "50Agg","51Agg", "52Agg")


    //Add in the zeros for the start of the year.


    for(h <- 1 to 3){
      val number = h.toString
      df3 = df3.withColumn(number, col("sum(4Agg)") * 0)
    }

    for(i <- 5 to 52){
      val nonAggNumber = i.toString
      val lastWeek = i - 1 //sum(5Agg)
      df3 = df3.withColumn(nonAggNumber, col(s"sum(${i}Agg)") - col(s"sum(${lastWeek}Agg)"))
    }
    df3 = df3.withColumnRenamed("sum(4Agg)", "4")
    df3 = df3.select("Name","1", "2", "3", "4","5", "6","7", "8","9", "10",
      "11", "12","13", "14","15", "16","17", "18", "19", "20","21", "22","23", "24",
      "25", "26","27", "28","29", "30","31", "32","33", "34","35", "36","37", "38",
      "39", "40","41", "42","43", "44","45", "46","47", "48","49", "50","51", "52")

   // df3.show(100)

    var rowDat = Row.empty
    var deathsArr = ArrayBuffer.empty[Row]

    for(i <- 1 to 52){
      deathsArr += Row(i, df3.first().get(i).asInstanceOf[Long].toInt)
    }
    val tableStructure = StructType(Array(  // Describe the data
      StructField("Week", IntegerType, false),
      StructField("Deaths", IntegerType, false)))
    var df4 = spark.createDataFrame(spark.sparkContext.parallelize(deathsArr), tableStructure)
    df4.show(100)
    saveDataFrameAsCSV(df4, "TexasCovidDeathsByWeek2020.csv")

  }

}
