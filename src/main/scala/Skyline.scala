import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, desc, least, max,min, rank, when}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, greatest, lit, struct}
import  org.apache.spark.sql.functions.expr


object bigdata {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("Skyline")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("Skyline")
      .getOrCreate()



    val df = sparkSession.read.option("header", "true").csv("//src//main//resource//Normal.csv")

    val X = df.select("id","0").withColumn("rank_X",rank().over(Window.orderBy("0")))
    val Y = df.select("id","1").withColumn("rank_Y",rank().over(Window.orderBy("1")))

    val Ranks =  X.join(Y, "id")
    val Max_X = Ranks.drop("0","1").filter("rank_X>rank_Y")//.drop("rank_Y")
    val Max_Y = Ranks.drop("0","1").filter("rank_X<rank_Y")//.drop("rank_X")

    //val Rank = Ranks.drop("0","1").withColumn("max", greatest("rank_X", "rank_Y"))

    val minx = Max_X.withColumn("Min_X", min("rank_X").over(Window.orderBy("rank_X")))
      .withColumn("Sub_X",expr("rank_X - Min_X")).withColumn("Min_Y", min("rank_Y").over(Window.orderBy("rank_Y")))
      .withColumn("Sub_Y",expr("rank_Y - Min_y")).withColumn("Dominated", least("Sub_X","Sub_Y"))
      .drop("rank_X","rank_Y","Min_X","Min_Y")
    val miny = Max_Y.withColumn("Min_X", min("rank_X").over(Window.orderBy("rank_X")))
      .withColumn("Sub_X",expr("rank_X - Min_X")).withColumn("Min_Y", min("rank_Y").over(Window.orderBy("rank_Y")))
      .withColumn("Sub_Y",expr("rank_Y - Min_y")).withColumn("Dominated", least("Sub_X","Sub_Y"))
      .drop("rank_X","rank_Y","Min_X","Min_Y")
    val Skyline  = miny.union(minx).filter("Dominated==0").drop("Sub_X","Sub_Y")




    println(df.show())
    println(X.show())
    println(Y.show())

    //println(Max_Y.show())
    //println(minx.show())
    //print(miny.show())
    //println(Skyline.show())
    //println(Skyline.count())
    //println(Max_X.show())




  }

}
