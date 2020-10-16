package com.styn.utils

import org.apache.spark.sql.SparkSession

/**
 * Created by STYN on 23-06-2020
 */
object SparkConfig {
  val spark = SparkSession.builder().master("local").config("spark.driver.host","localhost").enableHiveSupport().getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val hdfs = spark.sparkContext.hadoopConfiguration
}
