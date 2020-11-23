import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, collect_list, size, column, desc, expr, least, lit, max, min, rank, struct, udf, when}
import org.apache.spark.sql.expressions.Window
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark



 object bigdata {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[2]").setAppName("Skyline")
    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("Skyline")
      .getOrCreate()



    val df = sparkSession.read.option("header", "true").csv("src/main/Resource/Normal.csv")


    val X = df.select("id","0").withColumn("rank_X",rank().over(Window.orderBy("0")))
    val Y = df.select("id","1").withColumn("rank_Y",rank().over(Window.orderBy("1")))
    val Ranks =  X.join(Y, "id")


    //val mapRank = Ranks.rdd.map(row => row(0) -> (row.getDouble(2),row.getDouble(4))).collectAsMap()

    Ranks.createOrReplaceTempView("LEFT_RANK")
    Ranks.createOrReplaceTempView("RIGHT_RANK")
    //SQL JOIN
    val joinDF = sparkSession.sql("select l.id AS lid, r.id AS rid " +
      "from LEFT_RANK l, RIGHT_RANK r " +
      "where l.rank_Y < r.rank_Y and l.rank_X < r.rank_X ")

    val scoresDF = joinDF.groupBy("lid")
      .agg(collect_list("rid").alias("dominated_set"))
      .withColumn("score", size(col("dominated_set"))).drop("dominated_set")

    print(scoresDF.show())
//    scoresDF.write.csv("output/scores")


    val miny = Ranks.withColumn("Min", min("rank_Y").over(Window.orderBy("rank_X")))
    val sky = miny.filter(col("rank_Y")===col("Min"))
    val skyline = sky.drop("0","1","rank_X","rank_Y","Min")
    
    println(miny.show())
    println(sky.show())
    println(skyline.show())






  }

}
