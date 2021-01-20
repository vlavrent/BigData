import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}



 object bigdata {


  def SaveCSV(saveSkyline:RDD[(Double,Double,Double)]){
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)


    val conf = new SparkConf().setMaster("local[1]").setAppName("Skyline")
    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("Skyline")
      .getOrCreate()
    import sparkSession.implicits._

    val save = saveSkyline.toDF("0","1","id").coalesce(1)
    save.write.option("header", "true").csv("skyline10000000_2d.csv")


  }


  def task1(dataset_path:String): RDD[(Double,Double,Double)]  ={




    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)


    val conf = new SparkConf().setMaster("local[*]").setAppName("Skyline").set("spark.driver.memory", "8g")
    val sc = new SparkContext(conf)


    //Read csv and remove headers, set partitions same as the number of cores
    val rddFromFile = sc.textFile(dataset_path,6)
    val header = rddFromFile.first()
    val rdd1 = rddFromFile.filter(row => row != header).map(f=>{f.split(",")})


    //Map values to double type, create Tuples, sort by the sum of the values of 1st, 2nd and 3rd dimensions
    //Iterate each partition and find skyline
    //Collect the local skylines of all partitions
    val rdd = rdd1.map(n => {(n.map(_.toDouble))}).map(n=>Tuple4(n(0),n(1),n(2),(n(0)+n(1))))
      .sortBy(_._4,true)
      .mapPartitions( iterator => {

        var first = iterator.next()
        var sky : List[(Double,Double,Double,Double)] = List()
        sky = sky:+(first._1,first._2,first._3,first._4)

        while(iterator.hasNext) {
          var it = iterator.next()

          var flag = true
          var i = 0
          while (i<sky.size && flag==true){
            if((sky(i)._1<it._1 && sky(i)._2 <= it._2)
              || (sky(i)._1 <= it._1 && sky(i)._2 < it._2)
              )flag=false;
            else {flag=true}
            i = i+1
          }

          if(flag==true){sky= sky:+(it._1,it._2,it._3,it._4)}

        }
        (sky.toIterator)

      }).collect()




    //Sort Collected List according to the sum of the values of all dimensions
    //Extract the first point
    //Find the Global Skyline
    var skyline : List[(Double,Double,Double)] = List()
    val rdd2 = rdd.sortBy(_._4)
    val first = rdd2.take(1)

    first.map(x=> {skyline = skyline:+(x._1,x._2,x._3) })

    rdd2.drop(1).map(r=>{
      var flag = true
      var i = 0
      while (i<skyline.size && flag==true){
        if((skyline(i)._1<r._1 && skyline(i)._2 < r._2)
          || (skyline(i)._1 <= r._1 && skyline(i)._2 < r._2)
          ||(skyline(i)._1 < r._1 && skyline(i)._2 <= r._2))flag=false;
        else {flag=true}
        i = i+1
      }

      if(flag==true){skyline = skyline:+(r._1,r._2,r._3)}
    })
    //skyline.foreach(println)


    //Convert List to RDD and return Global Skyline
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
