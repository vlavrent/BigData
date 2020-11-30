import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, column, desc, expr, least, lit, max, min, rank, size, struct, udf, when}
import org.apache.spark.sql.expressions.Window
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark



 object bigdata {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[6]").setAppName("Skyline")
    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("Skyline")
      .getOrCreate()



    val df = sparkSession.read.option("header", "true").csv("src/main/Resource/Normal.csv")
      .select(col("0").alias("x"), col("1").alias("y"), col("id"))


      df.createOrReplaceTempView("df")

      //SQL JOIN
      val joinDF = sparkSession.sql("select l.id AS lid, r.id AS rid " +
        "from df l, df r " +
        "where l.x < r.x and l.y < r.y ")
      println(joinDF.show())

      val scored_df_without_zero = joinDF.groupBy(col("lid").alias("id"))
        .agg(size(collect_list("rid")).alias("score"))


      val scored_df = df.select("id").join(scored_df_without_zero, Seq("id"), "left_outer").na.fill(0, Seq("score"))


      //Skyline
      val search_null = df.select("id").join(scored_df_without_zero, Seq("id"), "left_outer")
      val keep_null = search_null.select(col("id")).where(col("score").isNull)
      val not_dom_null = keep_null.select("id").except(joinDF.select("rid").distinct())
      val skyline = joinDF.select("lid").except(joinDF.select("rid")).withColumnRenamed("lid", "id")
        .union(not_dom_null.select("id"))
      val skyline_scores = skyline.join(scored_df, "id")
    //println(skyline.sort(col("score").desc).show(10))
    //println(scoresDF.sort(col("score").desc).show(10))


    //scoresDF.write.csv("D://Vicky//Data and Web Science//Big Data//Dominate.csv")
    //skyline.write.csv("D://Vicky//Data and Web Science//Big Data//Skyline.csv")



  }

}
