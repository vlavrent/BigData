import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer

object Skyline_3d {

  def SaveCSV(saveSkyline:RDD[(Double,Double,Double,Double)]){
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)


    val conf = new SparkConf().setMaster("local[1]").setAppName("Skyline")
    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("Skyline")
      .getOrCreate()
    import sparkSession.implicits._

    val save = saveSkyline.coalesce(1).toDF("0","1","id")
    save.write.csv("skyline3d.csv")


  }

  def task1(dataset_path:String): RDD[(Double,Double,Double,Double)] ={
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)


    val conf = new SparkConf().setMaster("local[4]").setAppName("Skyline")
    val sc = new SparkContext(conf)

    //Read csv and remove headers
    val rddFromFile = sc.textFile(dataset_path,4)
    val header = rddFromFile.first()
    val rdd1 = rddFromFile.filter(row => row != header).map(f=>{f.split(",")})

    //Finding Local Skylines
    val rdd = rdd1.map(n => {(n.map(_.toDouble))}).map(n=>Tuple4(n(0),n(1),n(2),n(3)))
      .sortBy(_._1,true)
      .mapPartitions( iterator => {

        var miny = 2000.0
        var minz = 2000.0
        var sky : List[(Double,Double,Double,Double)] = List()
        while(iterator.hasNext) {
          var it = iterator.next()
          if(it._2<miny && it._3<minz){miny=it._2;minz=it._3;sky =sky:+ (it._1,it._2,it._3,it._4)}
          else if(it._2<miny && it._3>minz){miny=it._2;sky =sky:+ (it._1,it._2,it._3,it._4)}
          else if(it._2>miny && it._3<minz){minz=it._3;sky =sky:+ (it._1,it._2,it._3,it._4)}
        }
        (sky.toIterator)
        
      }).collect()


    //Finding Global Skyline
    var miny = 2000.0
    var minz = 2000.0
    var skyline : List[(Double,Double,Double,Double)] = List()
    

    rdd.sortBy(_._1).map(x=>{
      if(x._2<miny){miny=x._2;skyline = skyline:+ (x._1,x._2,x._3,x._4);}
      else if(x._2<miny && x._3>minz){miny=x._2;skyline =skyline:+ (x._1,x._2,x._3,x._4)}
      else if(x._2>miny && x._3<minz){minz=x._3;skyline =skyline:+ (x._1,x._2,x._3,x._4)}
      ; (skyline)}).foreach(println)

    //Convert List to RDD and return
    val saveSkyline = sc.parallelize(skyline)

    saveSkyline


  }

  def main(args: Array[String]): Unit = {

    val dataset_path = args(0)

    val t1 = System.nanoTime
    val saveSkyline = task1(dataset_path)
    val duration = (System.nanoTime - t1)
    print(duration)

    //Save Skyline
    SaveCSV(saveSkyline)



  }


}
