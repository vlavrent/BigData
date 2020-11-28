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



    //val mapRank = Ranks.rdd.map(row => row(0) -> (row.getDouble(2),row.getDouble(4))).collectAsMap()
    df.createOrReplaceTempView("RANK")
    //SQL JOIN
    val joinDF = sparkSession.sql("select l.id AS lid, r.id AS rid " +
      "from RANK l, RANK r " +
      "where l.x < r.x and l.y < r.y ")


    val scoresDF = joinDF.groupBy("lid")
      .agg(collect_list("rid").alias("dominated_set"))
      .withColumn("score", size(col("dominated_set"))).drop("dominated_set")

    val skyline = scoresDF.join(joinDF.select("lid").except(joinDF.select("rid")),Seq("lid"))

    //println(skyline.sort(col("score").desc).show(10))
    //println(scoresDF.sort(col("score").desc).show(10))


    //scoresDF.write.csv("D://Vicky//Data and Web Science//Big Data//Dominate.csv")
    //skyline.write.csv("D://Vicky//Data and Web Science//Big Data//Skyline.csv")



  }

}
