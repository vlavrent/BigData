import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{collect_list, size}
import org.apache.spark.sql.functions.{ col, max,min}


import scala.collection.mutable.ListBuffer

object Skyline_4d {
  def Find_mins(total: DataFrame,rank_x: DataFrame,sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val y_value = total.select(total("y").cast("double")).map(_.getDouble(0)).collect.toList
    val id = total.select(total("id").cast("Long")).map(_.getLong(0)).collect.toList
    var min_y = rank_x.select(max("y").cast("double")).first().getDouble(0)
    var tmin = min_y+1
    var skyline = new ListBuffer[Long]()

    for((y,idy)<-y_value zip id) if(y<tmin){tmin = y;skyline += idy}

    val values_sky = skyline.toList

    values_sky.toDF()

  }
  def task1(dataset_path :String): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("Skyline")
    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("Skyline")
      .getOrCreate()
    import sparkSession.implicits._
    //numPartitions



    val df = sparkSession.read.option("header", "true").csv(dataset_path)
      .select(col("0").alias("x"), col("1").alias("y"),col("2").alias("z"), col("3").alias("w"), col("id"))


    val sort_x = df.orderBy("x")
    val miny = sort_x.select(min("y")).first().getString(0)
    val minz = sort_x.select(min("z")).first().getString(0)
    val minw = sort_x.select(min("w")).first().getString(0)

    val RanksXY = sort_x.select("x","y","id")
    val RanksXZ = sort_x.select("x","z","id")
    val RanksXW = sort_x.select("x","w","id")

    val minxy = RanksXY.select(RanksXY("x").cast("String")).where("y=="+miny).first().getString(0)
    val minxz = RanksXZ.select(RanksXZ("x").cast("String")).where("z=="+minz).first().getString(0)
    val minxw = RanksXW.select(RanksXW("x").cast("String")).where("w=="+minw).first().getString(0)

    val FilterXY = RanksXY.filter("x<="+minxy)
    val FilterXZ = RanksXZ.filter("x<="+minxz)
    val FilterXW = RanksXW.filter("x<="+minxw)
    //print(filtered_ranks.show())
    //print(rank_x.show())

    val SkyXY = Find_mins(FilterXY,RanksXY,sparkSession)
    val SkyXZ = Find_mins(FilterXZ,RanksXZ,sparkSession)
    val SkyXW = Find_mins(FilterXW,RanksXW,sparkSession)
    val skyline = SkyXY.union(SkyXZ).union(SkyXW).groupBy(col("value"))
      .agg(size(collect_list("value")).alias("id"))
    print(skyline.show())
    //skyline.write.csv("skyline.csv")



  }
  def main(args: Array[String]): Unit = {

    val dataset_path = args(0)


    val t1 = System.nanoTime
    task1(dataset_path )
    val duration = (System.nanoTime - t1)
    print(duration)


  }

}



