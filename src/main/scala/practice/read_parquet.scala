package practice

import org.apache.spark._
import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col,column}
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.{col,column}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{StringType,LongType, DoubleType}

object read_parquet {
  def main(args:Array[String]){
    val spark = SparkSession.builder().appName("hi").config("spark.master", "local").getOrCreate()
    val load_parquet = spark.read.format("parquet").load("/Users/suprabhatsinha/Documents/Projects/Miscll/userdata.parquet")
    val print_data = load_parquet.show()
    println(print_data)
    
  }
}