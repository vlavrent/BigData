import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, column, desc, expr, least, lit, max, min, rank, struct, udf, when}
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


    val miny = Ranks.withColumn("Min", min("rank_Y").over(Window.orderBy("rank_X")))
    val sky = miny.filter(col("rank_Y")===col("Min"))
    val skyline = sky.drop("0","1","rank_X","rank_Y","Min")
    
    println(miny.show())
    println(sky.show())
    println(skyline.show())






  }

}
