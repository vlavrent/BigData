import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ SparkSession}



 object bigdata {


  def SaveCSV(saveSkyline:RDD[(Double,Double,Double)]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)


    val conf = new SparkConf().setMaster("local[1]").setAppName("Skyline")
    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("Skyline")
      .getOrCreate()
    import sparkSession.implicits._

    val save = saveSkyline.coalesce(1).toDF("0","1","id")
    save.write.csv("skyline.csv")


  }


  def task1(dataset_path:String): RDD[(Double,Double,Double)] ={



    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)


    val conf = new SparkConf().setMaster("local[4]").setAppName("Skyline")
    val sc = new SparkContext(conf)



    //Read csv and remove headers
    val rddFromFile = sc.textFile(dataset_path,2)
    val header = rddFromFile.first()
    val rdd1 = rddFromFile.filter(row => row != header).map(f=>{f.split(",")})
    println(rdd1.getNumPartitions)

    //Finding Local Skylines
     val rdd = rdd1.map(n => {(n.map(_.toDouble))}).map(n=>Tuple3(n(0),n(1),n(2)))
      .sortBy(_._1,true)
    .mapPartitions( iterator => {

      var miny = 2000.0
      var sky : List[(Double,Double,Double)] = List()
      while(iterator.hasNext) {
        var it = iterator.next()
        if(it._2<miny){miny=it._2;sky =sky:+ (it._1,it._2,it._3)}
      }
      (sky.toIterator)
    }).collect()

    //Finding Global Skyline
    var miny = 2000.0
    var skyline : List[(Double,Double,Double)] = List()
    rdd.sortBy(_._1).map(x=>{if(x._2<miny){miny=x._2;skyline = skyline:+ (x._1,x._2,x._3);}; (skyline)})

    //Convert List to RDD and return
    val saveSkyline = sc.parallelize(skyline)

    saveSkyline


  }


  def main(args: Array[String]): Unit = {

    val dataset_path = //

    val t1 = System.nanoTime
    val saveSkyline = task1(dataset_path)
    val duration = (System.nanoTime - t1)
    print(duration)


    //Save Skyline
    SaveCSV(saveSkyline)


  }
 }
