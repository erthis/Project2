package com.revature.main

import com.revature.main.Project2.{saveDataFrameAsCSV, spark}
import org.apache.spark.sql.functions.{col, not, when}


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
    df = df.withColumnRenamed("All  Cause", "Deaths")
      .withColumnRenamed("Jurisdiction of Occurrence", "Name")
      .withColumnRenamed("MMWR Year", "Year")
      .withColumnRenamed("MMWR Week", "Week")
      .withColumn("Deaths", col("Deaths").cast("double"))
    df = df.filter(df("Year") === "2019")
    df = df.select("Deaths", "Name", "Week")
   // df = df.filter(df("Name") =!= "UnitedStates") Not working, not sure why.
    df.show(1000)
    saveDataFrameAsCSV(df, "stateDeathsByWeek2019.csv")
    /*


  df = df.withColumnRenamed("5/2/21", "Deaths")
    .withColumnRenamed("Jurisdiction of Occurrence", "Name")
    .withColumn("Deaths", col("Deaths").cast("double"))
    .withColumn("Population", col("Population").cast("double"))
  // Sums the population and name columns, sorts by name
  df = df.groupBy(col("Name"))
    .sum("Population", "Deaths")
    .orderBy("Name")
  df = df.withColumn("fraction", col("sum(Deaths)")/col("sum(Population)"))
    .withColumn(  "Percent", col("fraction") * 100 )
    .drop("fraction")
  df = df.withColumn("Percent", when(col("Percent").isNull,0.0) otherwise (col("Percent")))
    .filter(not(df("Percent")===0.0))
  df.show(100)
  saveDataFrameAsCSV(df, "statePopDeathsPercent2.csv")


*/
  }

}
