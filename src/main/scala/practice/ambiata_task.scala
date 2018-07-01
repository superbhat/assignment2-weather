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
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.{col,column}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.types.{StringType,LongType, DoubleType}
import scala.math.sqrt

object ambiata_task {
  def main(args:Array[String]){
  //PREPARE SPARK SESSION   
       val spark = SparkSession.builder().appName("TASK APP")
          .config("spark.master", "local").getOrCreate()
  //PREPARING DATAFRAME ON SPARK CLUSTER 
  //OPTION("DELIMITER", "|")        
       val input_df = spark.read.csv("globe_data.csv")  
       
  //SPLITING THE OUT OF ORDER DATA INTO ORDER SCHEMA     
       val order_df = input_df.withColumn("_tmp", split(col("_c0"), "\\|")).
                               select(col("_tmp").getItem(0).as("Timestamp"),
                                      col("_tmp").getItem(1).as("Location"),
                                      col("_tmp").getItem(2).as("Temperature"),
                                      col("_tmp").getItem(3).as("Observatory")).drop("_tmp")
                                      
 //UPDATING THE SCHEMA DATA TYPE AS PER REQUIREMENT
 //TIMESTAMP FIELD AS TIMESTAMP DATATYPE
 //TEMPERATURE FIELD AS INTEGER.             
       val col_updt_df_ts   = order_df
                              .withColumn("TimeStamp", order_df.col("Timestamp").cast(TimestampType))
       val ts_updt_df       = col_updt_df_ts.withColumnRenamed("TimeStamp", "Timestamp")
       val col_updt_df_temp = ts_updt_df.withColumn("Temp", ts_updt_df.col("Temperature").cast(IntegerType))
       val drop_old_col_df  = col_updt_df_temp.drop("Temperature")
       val final_order_df   = drop_old_col_df.withColumnRenamed("Temp", "Temperature")
       
//INVALID DATA CHECK
//DROPPING THE RECORDS IN WHICH ALL COLUMNS VALUES ARE NULL
       final_order_df.where(col("Timestamp") === " " && col("Location") === " " && col("Temperature") === 0 && col("Observatory") === " ").drop()
       
//INVALID DATA CHECK FOR TEMPERATURE COLUMNS FOR DIFFERENT OBSERVATORY
//NOTE NULL VALUES ARE REPLACED WITH MEAN_TEMPERATURE CALCULATED FOR THAT PARTICULAR OBSERVATORY.
//COUNT VALUES WILL SAY IF ANY NULL OR INVALID VALUES PRESENT OR NOT       
       val count_val = final_order_df.filter(col("Temperature") === "null" || col("Temperature").isNull || col("Temperature") === 0 
                                          || col("Observatory").isNull || col("Observatory") === " " 
                                          || col("Location").isNull || col("Location") === " " || col("Location") === "0,0"
                                          || col("Timestamp").isNull || col("Timestamp") === " " ).count()
     
//CALCULATION MEAN TEMPERATURE FOR AU AND REPLACING NULL OR ZEROES VALUES WITH MEAN TEMPERATURE        
       val mean_temp_au = final_order_df.where(col("Observatory") === "AU").select(mean(final_order_df("Temperature"))).first()(0).toString.toDouble

//CALCULATION MEAN TEMPERATURE FOR FR AND REPLACING NULL OR ZEROES VALUES WITH MEAN TEMPERATURE 
       val mean_temp_fr = final_order_df.where(col("Observatory") === "FR").select(mean(final_order_df("Temperature"))).first()(0).toString.toDouble

//CALCULATION MEAN TEMPERATURE FOR US AND REPLACING NULL OR ZEROES VALUES WITH MEAN TEMPERATURE  
       val mean_temp_us = final_order_df.where(col("Observatory") === "US").select(mean(final_order_df("Temperature"))).first()(0).toString.toDouble

//CALCULATION MEAN TEMPERATURE FOR OTH AND REPLACING NULL OR ZEROES VALUES WITH MEAN TEMPERATURE   
       val mean_temp_oth = final_order_df.where(col("Observatory") === "OTH").select(mean(final_order_df("Temperature"))).first()(0).toString.toDouble

//UDF - FOR ELIMINATING NULL VALUE PRESENT IN TEMPERATURE COLUMN WITH THE RESPECTIVE OBSERVATORY MEAN TEMPERATURE.
//ASSUMPTIONS -- SAME LOGIC NEEDS TO BE DONE FOR OTHERS COLUMNS IN THE DATAFRAME TO GET ALL INVALID VALUES EDITTED.
 //ASSUMING COUNT VALUE MORE THAN 1 I.E. WE HAVE NULL VALUES IN OUR COLUMNS
                            val null_temp_rm_udf = udf((col1:Integer,col2:String) => if ((col1 == "null" || col1 == null || col1 == 0) && (col2 == "AU")) mean_temp_au else
                                                                                     if ((col1 == "null" || col1 == null || col1 == 0) && (col2 == "FR")) mean_temp_fr else
                                                                                     if ((col1 == "null" || col1 == null || col1 == 0) && (col2 == "US")) mean_temp_us else 
                                                                                     if ((col1 == "null" || col1 == null || col1 == 0) && (col2 == "OTH")) mean_temp_oth else (col1 - 0))                                                                                     
                            val null_rm_df = final_order_df.withColumn("Correct Temp", null_temp_rm_udf(col("Temperature"),col("Observatory")))       
                            val final_not_null_df = null_rm_df.drop("Temperature")
                                                              .withColumnRenamed("Correct Temp", "Temperature")                                      
         
//AFTER GETTING ALL THE NULL REPLACED, CALCULATING MEAN TEMPERATURE AS WHOLE AROUND THE GLOBE IN DEGREE CELCIUS  
      //FARENHITE TO CELCIUS FOR US OBSERVATORY
       val mean_temp_us_cel = ((mean_temp_us - 32) * .556)
       
      //KELVIN TO CELCIUS FOR FR AND OTHER OBSERVATORIES
       val mean_temp_fr_cel = mean_temp_fr - 273.15
       val mean_temp_oth_cel = mean_temp_oth - 273.15
       
//TOTAL MEAN TEMPERATURE AROUND THE GLOBE
       val mean_temperature = ((mean_temp_au + mean_temp_fr_cel + mean_temp_us_cel + mean_temp_oth) / 4)
       
//CREATING UDF WHICH WILL CREATE A TEMPERATURE IN CELCIUS COLUMN WHICH WILL HAVE ALL THE TEMPERATURES OF DIFFERENT OBSERVATORY INTO A NORMALISED TEMPERATURE VALUE IN CELCIUS
       val temp_in_cel_udf = udf((col1:String, col2:Double) => if(col1 == "AU") col2 else 
                                                               if (col1 == "US") ((col2 - 32) * 0.556) else (col2 - 273.15))
                                                               
//APPLYING UDF FUNCTION TO NON NULL DATAFRAME
        val updt_df_with_temp_col_df = final_not_null_df.withColumn("Temperature in Celcius" , temp_in_cel_udf(col("Observatory"),col("Temperature")))
        
//CALCULATE MINMUM TEPERATURE AROUND THE GLOBE
        val min_temp = updt_df_with_temp_col_df.select(min(updt_df_with_temp_col_df("Temperature in Celcius"))).first()(0).toString.toDouble
        
//CALCULATE MAXIMUM TEPERATURE AROUND THE GLOBE
        val max_temp = updt_df_with_temp_col_df.select(max(updt_df_with_temp_col_df("Temperature in Celcius"))).first()(0).toString.toDouble
        
//CALCULATING THE COUNT FOR THE DIFFERENT OBSERVATORIES
        val obs_cnt_au  = updt_df_with_temp_col_df.filter(updt_df_with_temp_col_df("Observatory") === "AU").count()
        val obs_cnt_fr  = updt_df_with_temp_col_df.filter(updt_df_with_temp_col_df("Observatory") === "FR").count()
        val obs_cnt_us  = updt_df_with_temp_col_df.filter(updt_df_with_temp_col_df("Observatory") === "US").count()
        val obs_cnt_oth = updt_df_with_temp_col_df.filter(updt_df_with_temp_col_df("Observatory") === "OTH").count()
        
//SCHEMA FOR ATMOSPHERIC DATAFRAME
        val schema_obs = new StructType(Array(
                      new StructField("Observatory", StringType, false),
                      new StructField("Temp Measurment", StringType, false),
                      new StructField("Distance",    StringType, false),
                      new StructField("Loc",         StringType, false)))
       
//CREATING DATAFRAME FOR OBSERVATORY DETAILS WHICH INCLUDES MEASUREMENT SPECIFIC TO OBSERVATORIES.       
        val obs_dtl_df = spark.read.format("csv").option("header","false").schema(schema_obs).load("obs_data.csv")

//JOINING LOG DATA FRAME WITH THE OBSERVATORY DATAFRAME TO GET A NORMALISED DATAFRAME WHICH PROVIDE NORMALISED DETAILS OF THE OBSERVATORIES IN THEIR SPECIFIC UNITS.        
        val log_data_join_obs_data_df = updt_df_with_temp_col_df.join(obs_dtl_df,"Observatory")
        
//FORMATTING THE DATAFRAME TO GET (X,Y) COORDINATES IN DIFFERENT COLUMN TO CALCUALTE THE DISTANCE TRAVELLED
        val split_location_df = log_data_join_obs_data_df.withColumn("_tmp", split(col("Location"), "\\,")).
                                                          withColumn("_tmp1", split(col("Loc"), "\\,")).
                                                          select(col("_tmp").getItem(0).as("X of Location"),
                                                                 col("_tmp").getItem(1).as("Y of Location"),
                                                                 col("_tmp1").getItem(0).as("X of Loc"),
                                                                 col("_tmp1").getItem(1).as("Y of Loc"),
                                                                 col("Observatory"),col("Timestamp"),col("Location"),
                                                                 col("Temperature"),col("Temperature in Celcius"),col("Temp Measurment"),col("Distance"),col("Loc"))
                                                         .drop("_tmp") 
                                                         
//LOGIC OF BELOW PART IS TWO UPDATE THE COLUMN DATA TYPE AS NEEDED        
        val location_updt_dtype_df   = split_location_df
                                      .withColumn("New X of Location", split_location_df.col("X of Location").cast(IntegerType))
                                      .withColumn("New Y of Location", split_location_df.col("Y of Location").cast(IntegerType))
                                      .withColumn("New X of Loc", split_location_df.col("X of Loc").cast(IntegerType))
                                      .withColumn("New Y of Loc", split_location_df.col("Y of Loc").cast(IntegerType))
                                      .drop("X of Location")
                                      .drop("Y of Location")
                                      .drop("X of Loc")
                                      .drop("Y of Loc")
                                      .withColumnRenamed("New X of Location", "X of Location")
                                      .withColumnRenamed("New Y of Location", "Y of Location") 
                                      .withColumnRenamed("New X of Loc", "X of Loc")
                                      .withColumnRenamed("New Y of Loc", "Y of Loc")
                                      
//LOGIC TO CALCULATE DISTANCE BETWEEN TWO POINTS - DISTANCE IN OBSERVATORY SPECIFIC DETAILS = SQUARE ROOT OF ((X1 - X2)*(X1 -X2) + (Y1 - Y2)*(Y1 - Y2))
       val dis_tra = udf((col1:String,col2:Integer,col3:Integer,col4:Integer,col5:Integer) => 
                          if(col1 == "AU") ((sqrt((col2 - col4)*(col2 - col4) + (col3 - col5)*(col3 - col5)))) else 
                          if(col1 == "FR") ((sqrt((col2 - col4)*(col2 - col4) + (col3 - col5)*(col3 - col5)))*1000) else
                          if(col1 == "US") ((sqrt((col2 - col4)*(col2 - col4) + (col3 - col5)*(col3 - col5)))*0.621371) else (sqrt((col2 - col4)*(col2 - col4) + (col3 - col5)*(col3 - col5))))
                          
//LOGIC TO CALCULATE DISTANCE BETWEEN TWO POINTS - DISTANCE IN KM = SQUARE ROOT OF ((X1 - X2)*(X1 -X2) + (Y1 - Y2)*(Y1 - Y2))                          
         val dis_tra_km = udf((col1:String,col2:Integer,col3:Integer,col4:Integer,col5:Integer) => 
                          if(col1 == "AU") ((sqrt((col2 - col4)*(col2 - col4) + (col3 - col5)*(col3 - col5)))) else 
                          if(col1 == "FR") ((sqrt((col2 - col4)*(col2 - col4) + (col3 - col5)*(col3 - col5)))) else
                          if(col1 == "US") ((sqrt((col2 - col4)*(col2 - col4) + (col3 - col5)*(col3 - col5)))) else (sqrt((col2 - col4)*(col2 - col4) + (col3 - col5)*(col3 - col5))))
 
//UDF LOGIC FOR CONCANTINATING TWO COLUMNS                          
       val concat_udf = udf((col1:Double,col2:String) => (col1+" "+col2))                   
       
//DATAFRAME PROVIDING DETAILS OF DISTANCE TRAVELLED FOR EVERY LOG FROM THEIR RESPECTIVE OBSERVATORY SPECIFIC AND GENERIC IN KM                                  
       val dis_col_df = location_updt_dtype_df.withColumn("Distance as per Obser", dis_tra(col("Observatory"),col("X of Location"),col("Y of Location"),col("X of Loc"),col("Y of Loc")))
                                              .withColumn("Distance in Km", dis_tra_km(col("Observatory"),col("X of Location"),col("Y of Location"),col("X of Loc"),col("Y of Loc"))) 
//UPDATING THE SCHEMA DATATYPES       
       val con_dis_df = dis_col_df.withColumn("Dis", dis_col_df.col("Distance as per Obser").cast(IntegerType))
                                              .drop("Distance as per Obser")
                                              .withColumnRenamed("Dis", "Distance as per Obser")
                                              .withColumn("Distance as per Observatory", concat_udf(col("Distance as per Obser"),col("Distance")))
                                              .withColumn("Temperature as per Observatory", concat_udf(col("Temperature"),col("Temp Measurment")))
                                              
//CALCULATE TOTAL DISTANCE TRAVELLED BY THE GLOBE
        val tot_dis_travl_by_globe =  dis_col_df.select(sum(col("Distance in Km"))).first()(0).toString.toDouble
        
//CREATING DATAFRAME WHICH WILL HAVE THE NORMALISED OUTPUT DATA FOR THE INPUT FROM THE LOG
        val final_df = con_dis_df.select(col("Observatory"),col("Location"),col("Temperature as per Observatory"),col("Distance as per Observatory"),col("Timestamp"))
        
//FORMATTING TIMESTAMP VALUE TO THE UTC FORMAT
       val new_df = final_df.withColumn("Time Stamp", date_format(col("Timestamp"),"yyyy-MM-dd'T'HH:mm:ss.SSS"))
       val output_df = new_df.drop("Timestamp")
                             .withColumnRenamed("Time Stamp", "Timestamp")
                             
//PRINTING THE OUTPUT AS A DATAFRAME                             
       println("Final data look " + output_df.show())
     
//ASKING USER TO REQUEST TO ENTER BELOW MENTIONED OPTIONS TO KNOW THE VALUES
//OPTION 1 = MINIMUM TEMPERATURE
//OPTION 2 = MAXIMUM TEMPERATURE
//OPTION 3 = MEAN TEMPERATURE
//OPTION 4 = NUMBER OF OBSERVATIONS FROM EACH OBSERVATORY
//OPTION 5 = TOTAL DISTANCE TRAVELLED BY GLOBE       
        println("PLEASE PROVIDE YOUR INPUT")
        println("OPTION 1 = MINIMUM TEMPERATURE")
        println("OPTION 2 = MAXIMUM TEMPERATURE")
        println("OPTION 3 = MEAN TEMPERATURE")
        println("OPTION 4 = NUMBER OF OBSERVATIONS FROM EACH OBSERVATORIES")
        println("OPTION 5 = TOTAL DISTANCE TRAVELLED BY GLOBE")
        val input_req = readInt()
        input_req match {
                  case (1) => println(" Minimum Temperature is " + min_temp.toInt + " CELCIUS")
                  case (2) => println(" Maximum Temperature is " + max_temp.toInt + " CELCIUS")
                  case (3) => println(" Mean Temperature is "    + mean_temperature.toInt + " CELCIUS")
                  case (4) => println(" OBSERVATIONS FOR AU "   + obs_cnt_au + " ,OBSERVATIONS FOR FR " + obs_cnt_fr + " ,OBSERVATIONS FOR US " + obs_cnt_us + " ,OBSERVATIONS FOR OTH " +obs_cnt_oth)
                  case (5) => println(" TOTAL DISTANCE TRAVELLED BY GLOBE " + tot_dis_travl_by_globe.toInt +" KM")
                  case (_) => println(" INCORRECT OPTION !! EXIT")
      }
  }
}