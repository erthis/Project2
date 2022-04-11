package com.revature.main

import com.revature.main.Project2.{saveDataFrameAsCSV, spark}
import org.apache.spark.sql.functions.{col, date_format, not, to_timestamp, when}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}


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
      //.withColumn("Deaths", col("Deaths").cast("double"))
    df = df.filter(df("Year") === "2019")
    df = df.select("Deaths", "Name", "Week")
   // df = df.filter(df("Name") =!= "UnitedStates") Not working, not sure why.
    //saveDataFrameAsCSV(df, "stateDeathsByWeek2019.csv")
    var df2 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("WeeklyDeaths2020.csv")

    df2 = df2.withColumnRenamed("All Cause", "Deaths")
      .withColumnRenamed("Jurisdiction of Occurrence", "Name")
      .withColumnRenamed("MMWR Year", "Year")
      .withColumnRenamed("MMWR Week", "Week")
   // df2 = df2.filter(df("Year") === "2020")
    df2.withColumn("Deaths", col("Deaths").cast("double"))
    df2 = df2.select("Deaths", "Name", "Week", "Week Ending Date")

    var df3 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("time_series_covid_19_deaths_US.csv")
    	 df3 = df3.withColumnRenamed("Admin2", "County")
			.withColumnRenamed("Province_state", "State")
			//.withColumnRenamed("5/2/21", "Deaths")
    df3 = df3.withColumnRenamed("1/25/20", "Week 4")
         .withColumnRenamed("2/1/20", "Week 5")
         .withColumnRenamed("2/8/20", "Week 6")
         .withColumnRenamed("2/15/20", "Week 7")
         .withColumnRenamed("2/22/20", "Week 8")
         .withColumnRenamed("2/29/20", "Week 9")
         .withColumnRenamed("3/7/20", "Week 10")
         .withColumnRenamed("3/14/20", "Week 11")
         .withColumnRenamed("3/21/20", "Week 12")
         .withColumnRenamed("3/28/20", "Week 13")
         .withColumnRenamed("4/4/20", "Week 14")
         .withColumnRenamed("4/11/20", "Week 15")
         .withColumnRenamed("4/18/20", "Week 16")
         .withColumnRenamed("4/25/20", "Week 17")
         .withColumnRenamed("5/2/20", "Week 18")
         .withColumnRenamed("5/9/20", "Week 19")
         .withColumnRenamed("5/16/20", "Week 20")
         .withColumnRenamed("5/23/20", "Week 21")
         .withColumnRenamed("5/30/20", "Week 22")
         .withColumnRenamed("6/6/20", "Week 23")
         .withColumnRenamed("6/13/20", "Week 24")
         .withColumnRenamed("6/20/20", "Week 25")
         .withColumnRenamed("6/27/20", "Week 26")
      .withColumnRenamed("7/4/20", "Week 27")
      .withColumnRenamed("7/11/20", "Week 28")
      .withColumnRenamed("7/18/20", "Week 29")
      .withColumnRenamed("7/25/20", "Week 30")
      .withColumnRenamed("8/1/20", "Week 31")
      .withColumnRenamed("8/8/20", "Week 32")
      .withColumnRenamed("8/15/20", "Week 33")
      .withColumnRenamed("8/22/20", "Week 34")
      .withColumnRenamed("8/29/20", "Week 35")
      .withColumnRenamed("9/5/20", "Week 36")
      .withColumnRenamed("9/12/20", "Week 37")
      .withColumnRenamed("9/19/20", "Week 38")
      .withColumnRenamed("9/26/20", "Week 39")
      .withColumnRenamed("10/3/20", "Week 40")
      .withColumnRenamed("10/10/20", "Week 41")
      .withColumnRenamed("10/17/20", "Week 42")
      .withColumnRenamed("10/24/20", "Week 43")
      .withColumnRenamed("10/31/20", "Week 44")
      .withColumnRenamed("11/7/20", "Week 45")
      .withColumnRenamed("11/14/20", "Week 46")
      .withColumnRenamed("11/21/20", "Week 47")
      .withColumnRenamed("11/28/20", "Week 48")
      .withColumnRenamed("12/5/20", "Week 49")
      .withColumnRenamed("12/12/20", "Week 50")
      .withColumnRenamed("12/19/20", "Week 51")
      .withColumnRenamed("12/26/20", "Week 52")
    df3 = df3.groupBy(col("State"))
      .sum("Population", "Week 4","Week 5", "Week 6","Week 7", "Week 8","Week 9", "Week 10",
        "Week 11", "Week 12","Week 13", "Week 14","Week 15", "Week 16","Week 17", "Week 18", "Week 19", "Week 20","Week 21", "Week 22","Week 23", "Week 24",
        "Week 25", "Week 26","Week 27", "Week 28","Week 29", "Week 30","Week 31", "Week 32","Week 33", "Week 34","Week 35", "Week 36","Week 37", "Week 38",
        "Week 39", "Week 40","Week 41", "Week 42","Week 43", "Week 44","Week 45", "Week 46","Week 47", "Week 48","Week 49", "Week 50","Week 51", "Week 52")



		//	.withColumn("Population", col("Population").cast("double"))
		//	.withColumn("Deaths", col("Deaths").cast("double"))



    df3.show(100)
    saveDataFrameAsCSV(df, "statePopDeaths2020ByWeek.csv")

    /*
    *
    * */
   // var df3 = df2.select("Week Ending Date")
    //df2.show()
    //df3.show(60)
   // saveDataFrameAsCSV(df, "stateDeathsByWeek2020.csv")
    //Got total death data for 2020 too, now.
    //var df4 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("time_series_covid_19_deaths_US.csv")
   // df4.printSchema()

   // var df5 = spark.emptyDataFrame
   // df5 = df2.select("Deaths", "Name", "Week", "Week Ending Date")
   // var distinctValuesDF = df4.select(df4("Province_State")).distinct.collectAsList()
   // for(i <- 0 until distinctValuesDF.size()){
   //   df5 = df
   // }
    //var stateNamesSet = distinctValuesDF.select("Flowers").as[String].collect().toSet

   // distinctValuesDF.show(1000)



    //saveDataFrameAsCSV(df, "stateDeathsByWeek2019.csv")
    /*
val flower_values = df2.select("Flowers").collectAsList()
val set2 = flower_values.map(_.getString(0)).toSet

        df.withColumn("input_timestamp",
      to_timestamp(col("input_timestamp")))
      .withColumn("week_of_year", date_format(col("input_timestamp"), "w"))
      .show(false)

      2020-01-04
      2020-01-11
      2020-01-18
      2020-01-25
      2020-02-01
      2020-02-08
      2020-02-15
      2020-02-22
      2020-02-29
      2020-03-07
      2020-03-14
      2020-03-21
      2020-03-28
      2020-04-04
      2020-04-11
      2020-04-18
      2020-04-25
      2020-05-02
      2020-05-09
      2020-05-16
      2020-05-23
      2020-05-30
      2020-06-06
      2020-06-13
      2020-06-20
      2020-06-27
      2020-07-04
      2020-07-11
      2020-07-18
      2020-07-25
      2020-08-01
      2020-08-08
      2020-08-15
      2020-08-22
      2020-08-29
      2020-09-05
      2020-09-12
      2020-09-19
      2020-09-26
      2020-10-03
      2020-10-10
      2020-10-17
      2020-10-24
      2020-10-31
      2020-11-07
      2020-11-14
      2020-11-21
      2020-11-28
      2020-12-05
      2020-12-12
      2020-12-19
      2020-12-26
      2021-01-02
      2021-01-09
      2021-01-16
      2021-01-23
      2021-01-30
      2021-02-06
      2021-02-13
      2021-02-20




1/22/20,1/23/20,1/24/20,1/25/20,1/26/20,1/27/20,1/28/20,1/29/20,1/30/20,1/31/20,2/1/20,2/2/20,
2/3/20,2/4/20,2/5/20,2/6/20,2/7/20,2/8/20,2/9/20,2/10/20,2/11/20,2/12/20,2/13/20,2/14/20,2/15/20,
2/16/20,2/17/20,2/18/20,2/19/20,2/20/20,2/21/20,2/22/20,2/23/20,2/24/20,2/25/20,2/26/20,2/27/20,2/28/20,
2/29/20,3/1/20,3/2/20,3/3/20,3/4/20,3/5/20,3/6/20,3/7/20,3/8/20,3/9/20,3/10/20,3/11/20,3/12/20,3/13/20,
3/14/20,3/15/20,3/16/20,3/17/20,3/18/20,3/19/20,3/20/20,3/21/20,3/22/20,3/23/20,3/24/20,3/25/20,3/26/20,
3/27/20,3/28/20,3/29/20,3/30/20,3/31/20,4/1/20,4/2/20,4/3/20,4/4/20,4/5/20,4/6/20,4/7/20,4/8/20,4/9/20,
4/10/20,4/11/20,4/12/20,4/13/20,4/14/20,4/15/20,4/16/20,4/17/20,4/18/20,4/19/20,4/20/20,4/21/20,4/22/20,
4/23/20,4/24/20,4/25/20,4/26/20,4/27/20,4/28/20,4/29/20,4/30/20,5/1/20,5/2/20,5/3/20,5/4/20,5/5/20,5/6/20,
5/7/20,5/8/20,5/9/20,5/10/20,5/11/20,5/12/20,5/13/20,5/14/20,5/15/20,5/16/20,5/17/20,5/18/20,5/19/20,5/20/20,
5/21/20,5/22/20,5/23/20,5/24/20,5/25/20,5/26/20,5/27/20,5/28/20,5/29/20,5/30/20,5/31/20,6/1/20,6/2/20,6/3/20,
6/4/20,6/5/20,6/6/20,6/7/20,6/8/20,6/9/20,6/10/20,6/11/20,6/12/20,6/13/20,6/14/20,6/15/20,6/16/20,6/17/20,6/18/20,
6/19/20,6/20/20,6/21/20,6/22/20,6/23/20,6/24/20,6/25/20,6/26/20,6/27/20,6/28/20,6/29/20,6/30/20,7/1/20,7/2/20,7/3/20,
7/4/20,7/5/20,7/6/20,7/7/20,7/8/20,7/9/20,7/10/20,7/11/20,7/12/20,7/13/20,7/14/20,7/15/20,7/16/20,7/17/20,7/18/20,7/19/20,
7/20/20,7/21/20,7/22/20,7/23/20,7/24/20,7/25/20,7/26/20,7/27/20,7/28/20,7/29/20,7/30/20,7/31/20,8/1/20,8/2/20,8/3/20,
8/4/20,8/5/20,8/6/20,8/7/20,8/8/20,8/9/20,8/10/20,8/11/20,8/12/20,8/13/20,8/14/20,8/15/20,8/16/20,8/17/20,8/18/20,8/19/20
,8/20/20,8/21/20,8/22/20,8/23/20,8/24/20,8/25/20,8/26/20,8/27/20,8/28/20,8/29/20,8/30/20,8/31/20,9/1/20,9/2/20,9/3/20,
9/4/20,9/5/20,9/6/20,9/7/20,9/8/20,9/9/20,9/10/20,9/11/20,9/12/20,9/13/20,9/14/20,9/15/20,9/16/20,9/17/20,9/18/20,
9/19/20,9/20/20,9/21/20,9/22/20,9/23/20,9/24/20,9/25/20,9/26/20,9/27/20,9/28/20,9/29/20,9/30/20,10/1/20,10/2/20,
10/3/20,10/4/20,10/5/20,10/6/20,10/7/20,10/8/20,10/9/20,10/10/20,10/11/20,10/12/20,10/13/20,10/14/20,10/15/20,10/16/20,
10/17/20,10/18/20,10/19/20,10/20/20,10/21/20,10/22/20,10/23/20,10/24/20,10/25/20,10/26/20,10/27/20,10/28/20,10/29/20,
10/30/20,10/31/20,11/1/20,11/2/20,11/3/20,11/4/20,11/5/20,11/6/20,11/7/20,11/8/20,11/9/20,11/10/20,11/11/20,11/12/20,
11/13/20,11/14/20,11/15/20,11/16/20,11/17/20,11/18/20,11/19/20,11/20/20,11/21/20,11/22/20,11/23/20,11/24/20,11/25/20,
11/26/20,11/27/20,11/28/20,11/29/20,11/30/20,12/1/20,12/2/20,12/3/20,12/4/20,12/5/20,12/6/20,12/7/20,12/8/20,12/9/20,
12/10/20,12/11/20,12/12/20,12/13/20,12/14/20,12/15/20,12/16/20,12/17/20,12/18/20,12/19/20,12/20/20,12/21/20,12/22/20,
12/23/20,12/24/20,12/25/20,12/26/20,12/27/20,12/28/20,12/29/20,12/30/20,12/31/20,1/1/21,1/2/21,1/3/21,1/4/21,
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
