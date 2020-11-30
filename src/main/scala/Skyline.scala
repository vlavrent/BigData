import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, column, desc, expr, least, lit, max, min, rank, size, struct, udf, when}
import org.apache.spark.sql.expressions.Window
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark



 object bigdata {
  /** Calculates dominance score for all points in the given dataframe taking into considerations the dominated
   *  points in the given dataframe
   *
   *  @param df given dataframe
   *  @param sparkSession the spark session to call sql
   *  @return a dataframe of points with their dominance score
   */
  def calculate_dominance_score(df:DataFrame,joinDF:DataFrame, sparkSession: SparkSession): DataFrame ={


    val scored_df_without_zero = joinDF.groupBy(col("lid").alias("id"))
      .agg(size(collect_list("rid")).alias("score"))

    val scored_df = df.select("id").join(scored_df_without_zero, Seq("id"), "left_outer").na.fill(0, Seq("score"))

    scored_df
  }

  def calculate_skyline_score(df: DataFrame,joinDF:DataFrame, sparkSession: SparkSession): DataFrame ={


    val scored_df = calculate_dominance_score(df,joinDF, sparkSession)

    val zero = scored_df.select("id","score").where("score == 0")
    val sky = zero.select("id").except(joinDF.select("rid").distinct())
    val skyline = joinDF.select("lid").except(joinDF.select("rid")).withColumnRenamed("lid", "id")
      .union(sky.select("id"))
    val skyline_scores = skyline.join(scored_df, "id")

    skyline_scores

  }

  def task1_3(k:Int,df:DataFrame,joinDF:DataFrame, sparkSession: SparkSession): Unit = {



    val skyline_score = calculate_skyline_score(df,joinDF, sparkSession)

    val skyline = skyline_score.select("id")
    print(sparkSession.time(skyline.sort(col("id").desc).take(k).mkString("(", ", ", ")")))
    print(sparkSession.time(skyline_score.sort(col("score").desc).take(k).mkString("(", ", ", ")")))

  }

  def task2(k:Int, df:DataFrame, joinDF:DataFrame, sparkSession: SparkSession): Unit ={



    //    val x_mean = df.select(avg("x")).first().getDouble(0)
    //    val y_mean = df.select(avg("y")).first().getDouble(0)
    //
    //    val quartile_1 = df.filter("x <" + x_mean + " AND  y <" + y_mean) //down left
    //    val quartile_2 = df.filter("x <" + x_mean + " AND  y >" + y_mean) //up left
    //    val quartile_3 = df.filter("x >" + x_mean + " AND  y <" + y_mean) //down right
    //    val quartile_4 = df.filter("x >" + x_mean + " AND  y >" + y_mean) //up right
    //
    //    val dominated_quartile_count = quartile_4.count()

    val scoresDf = calculate_dominance_score(df,joinDF, sparkSession)

    print(sparkSession.time(scoresDf.sort(col("score").desc).take(k).mkString("(", ", ", ")")))
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[6]").setAppName("Skyline")
    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("Skyline")
      .getOrCreate()

    val k = 10
    val dataset_path = "src/main/Resource/Normal.csv"

    val df = sparkSession.read.option("header", "true").csv(dataset_path)
      .select(col("0").alias("x"), col("1").alias("y"), col("id"))

    df.createOrReplaceTempView("df")
    //SQL JOIN
    val joinDF = sparkSession.sql("select l.id AS lid, r.id AS rid " +
      "from df l, df r " +
      "where l.x < r.x and l.y < r.y ")

    task1_3(k.toInt,df,joinDF,sparkSession)
    task2(k.toInt, df, joinDF, sparkSession)



 }
 }
