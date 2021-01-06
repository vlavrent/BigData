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

    val save = saveSkyline.coalesce(1).toDF("0","1","2","id")
    save.write.option("header", "true").csv("skyline1000000_3d.csv")


  }


  def task1(dataset_path:String): RDD[(Double,Double,Double,Double)] ={




    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)



    val conf = new SparkConf() .setAppName("Skyline").setMaster("local[8]").set("spark.executor.cores","8")
    val sc = new SparkContext(conf)//.set("spark.executor.cores","8")




    //Read csv and remove headers
    val rddFromFile = sc.textFile(dataset_path)
    val header = rddFromFile.first()
    val rdd1 = rddFromFile.filter(row => row != header).map(f=>{f.split(",")})

    println(rdd1.getNumPartitions)

    //Map values to double type, create Tuples, sort by the sum of the values of 1st, 2nd and 3rd dimensions
    val rdd = rdd1.map(n => {(n.map(_.toDouble))}).map(n=>Tuple5(n(0),n(1),n(2),n(3),(n(0)+n(1)+n(2))))
      .sortBy(_._5,true)
      .mapPartitions( iterator => {

        var first = iterator.next()

        var sky : List[(Double,Double,Double,Double,Double)] = List()
        sky = sky:+(first._1,first._2,first._3,first._4,first._5)

        while(iterator.hasNext) {
          var it = iterator.next()

          var flag = true
          var i = 0
          while (i<sky.size && flag==true){
            if((sky(i)._1<=it._1 && sky(i)._2 <= it._2 && sky(i)._3<it._3)
              ||(sky(i)._1<=it._1 && sky(i)._2<it._2 && sky(i)._3<=it._3)
              ||(sky(i)._1<it._1 && sky(i)._2<=it._2 && sky(i)._3<=it._3)
              )flag=false;
            else {flag=true}
            i = i+1
          }


          if(flag==true){sky= sky:+(it._1,it._2,it._3,it._4,it._5)}




        }
        (sky.toIterator)

      }).collect()


    

    var skyline : List[(Double,Double,Double,Double)] = List()


    val rdd2 = rdd.sortBy(_._5)
    val first = rdd2.take(1)


    first.map(x=> {skyline = skyline:+(x._1,x._2,x._3,x._4) })

    rdd2.drop(1).map(r=>{
      var flag = true
      var i = 0
      while (i<skyline.size && flag==true){
        if((skyline(i)._1<=r._1 && skyline(i)._2 <= r._2 && skyline(i)._3<r._3)
          ||(skyline(i)._1<=r._1 && skyline(i)._2 < r._2 && skyline(i)._3<=r._3)
          ||(skyline(i)._1<r._1 && skyline(i)._2 <= r._2 && skyline(i)._3<=r._3)
          )flag=false;
        else {flag=true}
        i = i+1
      }

      if(flag==true){skyline = skyline:+(r._1,r._2,r._3,r._4)}
    })


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
