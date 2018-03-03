package com.spark.streaming

import org.apache.spark.sql.SparkSession

object Assignment_19_1 extends App{

  val sparkSession = SparkSession.builder.master("local").appName("spark").config("spark.sql.warehouse.dir","file:///C:/Users").getOrCreate()
  val csvDF1 =   sparkSession.sqlContext.read.format("csv")
    .option("header", "true")
    .option("inferSchema", true).load("C:\\Users\\Nithin P\\Downloads\\Sports_data.txt")

  csvDF1.createOrReplaceTempView("Sports")
  //1)What are the total number of gold medal winners every year
  val input1 = sparkSession.sqlContext.sql("select year,count(*) as winners FROM Sports where medal_type = 'gold' group by year").show()
  //2)How many silver medals have been won by USA in each sport
  val input2 =  sparkSession.sqlContext.sql("select sports,count(*) from sports where country = 'USA' and medal_type = 'silver' " +
    "group by sports")
    .show()

}
