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

    val find_worst = udf((row:Row) => {
      val x = row.getInt(0)
      val y = row.getInt(1)
      if (x > y)
        "y"
      else
        "x"
    })

    val worst_df = Ranks.drop("0","1")
      .withColumn("worst", find_worst(struct(col("rank_X"), col("rank_Y"))))//.drop("rank_Y")

    val Max_X = worst_df.filter(col("worst") === "x")
    val Max_Y =  worst_df.filter(col("worst") === "y")//.drop("rank_X")

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



//
//    println(df.show())
//    println(X.show())
//    println(Y.show())

    println("Max_X\n", Max_X.show())
    println("Max_Y\n", Max_Y.show())
    println("minx\n",minx.show())
    print("miny\n",miny.show())
    println("Skyline\n",Skyline.show())
    println(Skyline.count())





  }

}
