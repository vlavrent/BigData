import java.lang.Thread.sleep

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{avg, broadcast, col, collect_list, lit, max, mean, not, size, udf}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.sys.exit


object dominanceScore {


  /** Calculates dominance score for all points in the given dataframe taking into considerations the dominated
   *  points in the given dataframe
   *
   *  @param df given dataframe
   *  @param sparkSession the spark session to call sql
   *  @return a dataframe of points with their dominance score
   */
  def calculate_dominance_score(df:DataFrame, sparkSession: SparkSession): DataFrame ={

    //JOIN
    val joinDF = df.as("df1")
      .join(df.as("df2"), !(col("df1.flag") === "dl" && col("df2.flag") === "ur")
    && col("df1.x") < col("df2.x") &&  col("df1.y") < col("df2.y"))
      .select(col("df1.id").alias("lid"),
        col("df2.id").alias("rid"),
        col("df1.initial_score").alias("initial_score"))



    val scored_df_without_zero = joinDF.groupBy(col("lid").alias("id"))
      .agg(size(collect_list("rid")).alias("counted_score"),
        max(col("initial_score")).alias("initial_score"))
      .withColumn("score", col("initial_score") + col("counted_score"))
      .select(col("id"), col("score"))

    val scored_df = df.select("id").join(scored_df_without_zero, Seq("id"), "left_outer").na.fill(0, Seq("score"))

    scored_df
  }

  def task2(k:Int, dataset_path:String): Unit ={

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("Skyline").set("spark.executor.memory", "12g")
    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("Skyline")
      .getOrCreate()


    val df = sparkSession.read.option("header", "true").csv("src/main/Resource/Normal.csv")
      .select(col("0").cast(DoubleType).alias("x"), col("1").cast(DoubleType).alias("y"), col("id"))

    val x_mean = df.select(avg("x")).first().getDouble(0)
    val y_mean = df.select(avg("y")).first().getDouble(0)

    def flag_point(x_mean:Double, y_mean:Double) = udf((x:Double, y:Double) => {

      if(x < x_mean && y < y_mean)
        "dl"
      else if (x < x_mean && y > y_mean)
        "ul"
      else if (x > x_mean && y < y_mean)
        "dr"
      else
        "ur"
    })

    val flagged_df = df.
      withColumn("flag", flag_point(x_mean,y_mean)(col("x"), col("y")))

//    val quartile_1 = df.filter("x <" + x_mean + " AND  y <" + y_mean) //down left
//    val quartile_2 = df.filter("x <" + x_mean + " AND  y >" + y_mean) //up left
//    val quartile_3 = df.filter("x >" + x_mean + " AND  y <" + y_mean) //down right
//    val quartile_4 = df.filter("x >" + x_mean + " AND  y >" + y_mean) //up right

    val dominated_quartile_count = flagged_df.filter(col("flag") === "ur").count()

    def initialize_score(score_for_dl:Long) = udf((flag:String) => {
      if (flag == "dl")
        score_for_dl.toInt
      else
        0
    })
    val initial_scored_df = flagged_df.
      withColumn("initial_score", initialize_score(dominated_quartile_count)(col("flag")))

    val scoresDf = calculate_dominance_score(initial_scored_df, sparkSession)

//    sparkSession.time(scoresDf.sort(col("score").desc).limit(5).write.format("com.databricks.")

    print(sparkSession.time(scoresDf.sort(col("score").desc).take(k).mkString("(", ", ", ")")))
  }

  def main(args: Array[String]): Unit = {

    val k = args(0)
    val dataset_path = args(1)

    task2(k.toInt, dataset_path)
  }


}
