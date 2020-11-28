import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, collect_list, size}
import org.apache.spark.sql.{DataFrame, SparkSession}


object dominanceScore {


  /** Calculates dominance score for all points in the given dataframe taking into considerations the dominated
   *  points in the given dataframe
   *
   *  @param df given dataframe
   *  @param sparkSession the spark session to call sql
   *  @return a dataframe of points with their dominance score
   */
  def calculate_dominance_score(df:DataFrame, sparkSession: SparkSession): DataFrame ={

    df.createOrReplaceTempView("df")

    //SQL JOIN
    val joinDF = sparkSession.sql("select l.id AS lid, r.id AS rid " +
      "from df l, df r " +
      "where l.x < r.x and l.y < r.y ")

    val scored_df_without_zero = joinDF.groupBy(col("lid").alias("id"))
      .agg(size(collect_list("rid")).alias("score"))

    val scored_df = df.select("id").join(scored_df_without_zero, Seq("id"), "left_outer").na.fill(0, Seq("score"))

    scored_df
  }

  def task2(k:Int, dataset_path:String): Unit ={

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[6]").setAppName("Skyline")
    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("Skyline")
      .getOrCreate()


    val df = sparkSession.read.option("header", "true").csv("src/main/Resource/Normal.csv")
      .select(col("0").alias("x"), col("1").alias("y"), col("id"))

    //    val x_mean = df.select(avg("x")).first().getDouble(0)
    //    val y_mean = df.select(avg("y")).first().getDouble(0)
    //
    //    val quartile_1 = df.filter("x <" + x_mean + " AND  y <" + y_mean) //down left
    //    val quartile_2 = df.filter("x <" + x_mean + " AND  y >" + y_mean) //up left
    //    val quartile_3 = df.filter("x >" + x_mean + " AND  y <" + y_mean) //down right
    //    val quartile_4 = df.filter("x >" + x_mean + " AND  y >" + y_mean) //up right
    //
    //    val dominated_quartile_count = quartile_4.count()

    val scoresDf = calculate_dominance_score(df, sparkSession)

    print(sparkSession.time(scoresDf.sort(col("score").desc).take(k).mkString("(", ", ", ")")))
  }

  def main(args: Array[String]): Unit = {

    val k = args(0)
    val dataset_path = args(1)

    task2(k.toInt, dataset_path)
  }


}
