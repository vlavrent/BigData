import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, col, collect_list, column, desc, expr, least, lit, max, min, rank, size, struct, udf, when}
import org.apache.spark.sql.expressions.Window
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import scala.collection.mutable.ListBuffer



 object bigdata {


   def addColumnIndex(df: DataFrame, sparkSession: SparkSession) = {
     sparkSession.sqlContext.createDataFrame(
       df.rdd.zipWithIndex.map {
         case (row, index) => Row.fromSeq(row.toSeq :+ index+1)
       },
       // Create schema for index column
       StructType(df.schema.fields :+ StructField("index", LongType, false)))

   }

   def Find_mins(df: DataFrame,res: DataFrame,sparkSession: SparkSession) = {
     import sparkSession.implicits._


     val please = res.select("rank_y").map(_.getLong(0)).collect.toList
     var min_y = df.count()
     var values = new ListBuffer[Long]()


     for (name <- please) if(name<min_y){min_y = name ; values += name}
     val values_sky = values.toList
     values_sky.toDF()

   }
   def task1(dataset_path:String): Unit ={

     Logger.getLogger("org").setLevel(Level.WARN)
     Logger.getLogger("akka").setLevel(Level.WARN)

     val conf = new SparkConf().setMaster("local[10]").set("spark.executor.cores", "5").set("spark.executor.instances", "4").setAppName("Skyline")
     val sparkSession = SparkSession.builder
       .config(conf = conf)
       .appName("Skyline")
       .getOrCreate()
     import sparkSession.implicits._


     val k = 10

     val df = sparkSession.read.option("header", "true").csv(dataset_path)
       .select(col("0").alias("x"), col("1").alias("y"), col("id"))


     //In Normal Distribution
     //val x_mean = df.select(avg("x")).first().getDouble(0)
     //val y_mean = df.select(avg("y")).first().getDouble(0)
     //val quartile_1 = df.filter("x <" + x_mean + " AND  y <" + y_mean) //down left
     //val quartile_2 = df.filter("x <" + x_mean + " AND  y >" + y_mean) //up left
     //val quartile_3 = df.filter("x >" + x_mean + " AND  y <" + y_mean)
     //val sky = quartile_1.union(quartile_2).union(quartile_3)


     val sky = df


     val rank_x = sky.sort("x").select("id")
     val X_RANK = addColumnIndex(rank_x,sparkSession).withColumnRenamed("index","rank_x")

     val rank_y = sky.sort("y").select("id")
     val Y_RANK = addColumnIndex(rank_y,sparkSession).withColumnRenamed("index","rank_y")

     val Total_Ranks = X_RANK.join(Y_RANK, "id").sort("rank_x")
     val miny = Total_Ranks.select(Total_Ranks("rank_x").cast("int")).where("rank_y==1").first().getInt(0)
     var minx = Total_Ranks.select(Total_Ranks("rank_y").cast("int")).where("rank_x==1").first().getInt(0)
     val Reduced_Ranks = Total_Ranks.filter("rank_x <= "+miny)


     val res = Reduced_Ranks.filter("rank_y >= 1 AND rank_y<="+minx).sort("rank_x")
     //val skyline = skyline(df,res,SparkSession)
     val y_mins = Find_mins(df,res,sparkSession).select(col ("value").alias("rank_y"))
     val skyline = res.join(y_mins,"rank_y").drop("rank_y","rank_x")
     //println(skyline.show())
     print(sparkSession.time(skyline.sort(col("score").desc).take(k).mkString("(", ", ", ")")))

   }

   def main(args: Array[String]): Unit = {

     val k = args(0)
     val dataset_path = "src/main/Resource/Normal.csv"

     task1(dataset_path)


   }



 }
