import bigdata.{Find_mins, addColumnIndex}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, size}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import scala.collection.mutable.ListBuffer

object Skyline_3d {

  def addColumnIndex(df: DataFrame, sparkSession: SparkSession) = {
    sparkSession.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map {
        case (row, index) => Row.fromSeq(row.toSeq :+ index+1)
      },
      // Create schema for index column
      StructType(df.schema.fields :+ StructField("index", LongType, false)))

  }

  def Find_mins(df: DataFrame,res: DataFrame,sparkSession: SparkSession,col_name:String) = {
    import sparkSession.implicits._


    val please = res.select(col(col_name)).map(_.getLong(0)).collect.toList
    var min_y = df.count()+1
    var values = new ListBuffer[Long]()


    for (name <- please) if(name<min_y){min_y = name ; values += name}
    val values_sky = values.toList
    values_sky.toDF()

  }

  def task1(dataset_path:String): Unit ={

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("Skyline")
    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("Skyline")
      .getOrCreate()
    import sparkSession.implicits._



    val df = sparkSession.read.option("header", "true").csv("src/main/Resource/Normal3d.csv")
      .select(col("0").alias("x"), col("1").alias("y"),col("2").alias("z"), col("id"))


    //val x_mean = df.select(avg("x")).first().getDouble(0)
    //val y_mean = df.select(avg("y")).first().getDouble(0)
    //val z_mean = df.select(avg("z")).first().getDouble(0)


    //val quartile_1 = df.filter("x <" + x_mean + " AND  y <" + y_mean) //down left
    //val quartile_2 = df.filter("x <" + x_mean + " AND  y >" + y_mean) //up left
    //val quartile_3 = df.filter("x >" + x_mean + " AND  y <" + y_mean)


    //val sky = quartile_1.union(quartile_2).union(quartile_3)
    val sky = df


    val rank_x = sky.sort("x").select("id")
    val X_RANK = addColumnIndex(rank_x,sparkSession).withColumnRenamed("index","rank_x")

    val rank_y = sky.sort("y").select("id")
    val Y_RANK = addColumnIndex(rank_y,sparkSession).withColumnRenamed("index","rank_y")

    val rank_z = sky.sort("z").select("id")
    val Z_RANK = addColumnIndex(rank_z,sparkSession).withColumnRenamed("index","rank_z")

    val Total_Ranks_XY = X_RANK.join(Y_RANK, "id").sort("rank_x")
    val Total_Ranks_XZ = X_RANK.join(Z_RANK, "id").sort("rank_x")



    val miny = Total_Ranks_XY.select(Total_Ranks_XY("rank_x").cast("int")).where("rank_y==1").first().getInt(0)
    var minxy = Total_Ranks_XY.select(Total_Ranks_XY("rank_y").cast("int")).where("rank_x==1").first().getInt(0)
    val Reduced_Ranks_XY = Total_Ranks_XY.filter("rank_x <= "+miny)
    val res_xy = Reduced_Ranks_XY.filter("rank_y >= 1 AND rank_y<="+minxy).sort("rank_x")
    val y_mins = Find_mins(df,res_xy,sparkSession,"rank_y").select(col ("value").alias("rank_y"))

    val minz = Total_Ranks_XZ.select(Total_Ranks_XZ("rank_x").cast("int")).where("rank_z==1").first().getInt(0)
    var minxz = Total_Ranks_XZ.select(Total_Ranks_XZ("rank_z").cast("int")).where("rank_x==1").first().getInt(0)
    val Reduced_Ranks_XZ = Total_Ranks_XZ.filter("rank_x <= "+minz)
    val res_xz = Reduced_Ranks_XZ.filter("rank_z >= 1 AND rank_z<="+minxz).sort("rank_x")
    val z_mins = Find_mins(df,res_xz,sparkSession,"rank_z").select(col ("value").alias("rank_z"))

    val skyline_xy = res_xy.join(y_mins,"rank_y").drop("rank_y","rank_x")
    val skyline_xz = res_xz.join(z_mins,"rank_z").drop("rank_z","rank_x")
    val skyline = skyline_xy.union(skyline_xz).groupBy(col("id"))
      .agg(size(collect_list("id")).alias("num")).drop("num")
    println(skyline.show())

  }

  def main(args: Array[String]): Unit = {

    val dataset_path = args(1)

    task1(dataset_path)


  }


}
