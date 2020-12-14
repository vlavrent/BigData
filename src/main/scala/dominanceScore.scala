import java.lang.Thread.sleep

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{avg, broadcast, col, collect_list, lit, max, mean, not, size, sum, udf}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.sys.exit


object dominanceScore {


  /** Calculates dominance score for all points in the given dataframe taking into considerations the dominated
   *  points in the given dataframe
   *
   *  @param df1 given dataframe
   *  @param sparkSession the spark session to call sql
   *  @return a dataframe of points with their dominance score
   */
  def calculate_dominance_score(df1:DataFrame, df2:DataFrame, sparkSession: SparkSession): DataFrame ={

    //JOIN !(col("df1.flag") === "dl" && col("df2.flag") === "ur")
    val joinDF = df1.as("df1")
      .join(df2.as("df2"), (col("df1.x") < col("df2.x") &&  col("df1.y") < col("df2.y"))
      ||  (col("df2.x") < col("df1.x") &&  col("df2.y") < col("df1.y")))

    val left_dominant_joinDF = joinDF.select(col("df1.id").alias("lid"),
      col("df2.id").alias("rid"))
      .where(col("df1.x") < col("df2.x") &&  col("df1.y") < col("df2.y"))

    val right_dominant_joinDF = joinDF.select(col("df2.id").alias("lid"),
      col("df1.id").alias("rid"))
      .where(col("df2.x") < col("df1.x") &&  col("df2.y") < col("df1.y"))

    val final_joinDF = left_dominant_joinDF.union(right_dominant_joinDF).distinct()

    val scored_df_without_zero = final_joinDF.groupBy(col("lid").alias("id"))
      .agg(size(collect_list("rid")).alias("score"))
      .select(col("id"), col("score"))

    val scored_df = df1.select("id").join(scored_df_without_zero, Seq("id"), "full_outer").na.fill(0, Seq("score"))

    scored_df
  }

  def calculate_dominance_score_in_quartile(df1:DataFrame, sparkSession: SparkSession, scoreToAdd:Int): DataFrame ={

    //JOIN !(col("df1.flag") === "dl" && col("df2.flag") === "ur")
    val joinDF = df1.as("df1")
      .join(df1.as("df2"), (col("df1.x") < col("df2.x") &&  col("df1.y") < col("df2.y")))
      .select(col("df1.id").alias("lid"),
      col("df2.id").alias("rid"))


    val scored_df_without_zero = joinDF.groupBy(col("lid").alias("id"))
      .agg(size(collect_list("rid")).alias("counted_score"))
      .withColumn("score", lit(scoreToAdd) + col("counted_score"))
      .select(col("id"), col("score"))

    val scored_df = df1.select("id").join(scored_df_without_zero, Seq("id"), "full_outer").na.fill(scoreToAdd, Seq("score"))

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

    //correlated/correlated50000.csv
    //uniform/uniform1000.csv
    val df = sparkSession.read.option("header", "true").csv("uniform/uniform1000.csv")
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

//    val flagged_df = df.
//      withColumn("flag", flag_point(x_mean,y_mean)(col("x"), col("y")))

    val quartile_1 = df.filter("x >" + x_mean + " AND  y >" + y_mean) //up right
    val quartile_2 = df.filter("x <" + x_mean + " AND  y >" + y_mean) //up left
    val quartile_3 = df.filter("x <" + x_mean + " AND  y <" + y_mean) //down left
    val quartile_4 = df.filter("x >" + x_mean + " AND  y <" + y_mean) //down right

    val quartile_1_count = quartile_1.count()

    def initialize_score(score_for_dl:Long) = udf((flag:String) => {
      if (flag == "dl")
        score_for_dl.toInt
      else
        0
    })

//    val initialized_quartile_1 = quartile_1.withColumn("initial_score", lit(0))
//    val initialized_quartile_2 = quartile_2.withColumn("initial_score", lit(0))
//    val initialized_quartile_3 = quartile_3.withColumn("initial_score", lit(0))
//    val initialized_quartile_4 = quartile_4.withColumn("initial_score", lit(0))
//    val initial_scored_df = flagged_df.
//      withColumn("initial_score", initialize_score(dominated_quartile_count)(col("flag")))

//    print(initialized_quartile_1.show())
//    print(initialized_quartile_2.show())
//    print(initialized_quartile_3.show())
//    print(initialized_quartile_4.show())


    val scored_quartile_2_3_1 = calculate_dominance_score(quartile_2,quartile_2.union(quartile_3).union(quartile_1),
      sparkSession)

    val scored_quartile_4_3_1 = calculate_dominance_score(quartile_4, quartile_4.union(quartile_3).union(quartile_1),
      sparkSession)

    val scored_quartile_1 = calculate_dominance_score_in_quartile(quartile_1, sparkSession, 0)
    val scored_quartile_3 = calculate_dominance_score_in_quartile(quartile_3, sparkSession, quartile_1_count.toInt)


    val scoresDf = scored_quartile_2_3_1.union(scored_quartile_4_3_1).union(scored_quartile_1).union(scored_quartile_3)
      .groupBy(col("id")).agg(sum(col("score")).alias("score"))


//    sparkSession.time(scoresDf.sort(col("score").desc).limit(5).write.format("com.databricks.")

    print(sparkSession.time(scoresDf.sort(col("score").desc).take(k).mkString("(", ", ", ")")))
  }

  def main(args: Array[String]): Unit = {

    val k = args(0)
    val dataset_path = args(1)

    task2(k.toInt, dataset_path)
  }


}
