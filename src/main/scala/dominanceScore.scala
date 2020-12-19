import java.lang.Thread.sleep
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{avg, broadcast, col, collect_list, lit, max, mean, not, size, sum, udf}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer
import scala.sys.exit


object dominanceScore {


	/** Calculates dominance score for all points in the given dataframe taking into considerations the dominated
	 *  points in the given dataframe
	 *
	 *  @param df1 given dataframe
	 *  @param sparkSession the spark session to call sql
	 *  @return a dataframe of points with their dominance score
	 */
	def calculate_dominance_score(df1:DataFrame, df2:DataFrame, sparkSession: SparkSession): DataFrame ={

		//JOIN !(col("df1.flag") === "dl" && col("df2.flag") === "ur")
		val joinDF = df1.as("df1")
			.join(df2.as("df2"), (col("df1.x") < col("df2.x") &&  col("df1.y") < col("df2.y"))
				||  (col("df2.x") < col("df1.x") &&  col("df2.y") < col("df1.y")))

		val left_dominant_joinDF = joinDF.select(col("df1.id").alias("lid"),
			col("df2.id").alias("rid"))
			.where(col("df1.x") < col("df2.x") &&  col("df1.y") < col("df2.y"))

		val right_dominant_joinDF = joinDF.select(col("df2.id").alias("lid"),
			col("df1.id").alias("rid"))
			.where(col("df2.x") < col("df1.x") &&  col("df2.y") < col("df1.y"))

		joinDF.unpersist()
		val final_joinDF = left_dominant_joinDF.union(right_dominant_joinDF).distinct()
		left_dominant_joinDF.unpersist()
		right_dominant_joinDF.unpersist()

		val scored_df_without_zero = final_joinDF.groupBy(col("lid").alias("id"))
			.agg(size(collect_list("rid")).alias("score"))
			.select(col("id"), col("score"))
		final_joinDF.unpersist()

		val scored_df = df1.select("id").join(scored_df_without_zero, Seq("id"), "full_outer").na.fill(0, Seq("score"))
		scored_df_without_zero.unpersist()
		scored_df
	}

	def calculate_dominance_score_in_quartile(df1:DataFrame, sparkSession: SparkSession, scoreToAdd:Int): DataFrame ={

		//JOIN !(col("df1.flag") === "dl" && col("df2.flag") === "ur")
		val joinDF = df1.as("df1")
			.join(df1.as("df2"), (col("df1.x") < col("df2.x") &&  col("df1.y") < col("df2.y")))
			.select(col("df1.id").alias("lid"),
				col("df2.id").alias("rid"))


		val scored_df_without_zero = joinDF.groupBy(col("lid").alias("id"))
			.agg(size(collect_list("rid")).alias("counted_score"))
			.withColumn("score", lit(scoreToAdd) + col("counted_score"))
			.select(col("id"), col("score"))
		joinDF.unpersist()
		val scored_df = df1.select("id").join(scored_df_without_zero, Seq("id"), "full_outer").na.fill(scoreToAdd, Seq("score"))
		scored_df_without_zero.unpersist()
		scored_df
	}

	def create_grid_axis(axis_mean:Double): List[Double] ={
		val axis_point4 = axis_mean
		val axis_point2 = axis_point4 / 2.0
		val axis_point1 = axis_point2 / 2.0
		val axis_point3 = (axis_point2 / 2.0) + axis_point2

		val axis_point6 = axis_point2 + axis_point4
		val axis_point5 = axis_point1 + axis_point4
		val axis_point7 = axis_point3 + axis_point4

		List(axis_point1, axis_point2, axis_point3, axis_point4, axis_point5, axis_point6, axis_point7)
	}

	def create_grid_cells_to_check(x_axis_size:Int, y_axis_size:Int): ListBuffer[(Int,Int)] ={
		var grid_cells_to_check = new ListBuffer[(Int, Int)]()

		for(grid_point <- (0 to x_axis_size) zip (0 to y_axis_size)){

			grid_cells_to_check += Tuple2(grid_point._1, grid_point._2)
			if (grid_point._1 - 1 < 0  && grid_point._2 - 1 < 0){

				grid_cells_to_check += Tuple2(grid_point._1 + 1, grid_point._2)
				grid_cells_to_check += Tuple2(grid_point._1, grid_point._2 + 1)
			}
			else if(grid_point._1  < x_axis_size  && grid_point._2  < y_axis_size){

				var j = grid_point._2 + 1
				breakable(
					for (i <- grid_point._1 - 1 to 0 by -1){
						grid_cells_to_check += Tuple2(i, j)
						j += 1
						if(j > y_axis_size)
							break()
					}
				)

				var i = grid_point._1 + 1
				breakable(
					for (j <- grid_point._2 - 1 to 0 by -1){
						grid_cells_to_check += Tuple2(i, j)
						i += 1
						if(i > x_axis_size)
							break()
					}
				)

				// include odd cells
				var k = grid_point._1 + 1
				breakable(
					for (h <- grid_point._2 to 0 by -1){
						grid_cells_to_check += Tuple2(k, h)
						k += 1
						if(k > x_axis_size)
							break()
					}
				)

				var h = grid_point._2 + 1
				breakable(
					for (k <- grid_point._1 to 0 by -1){
						grid_cells_to_check += Tuple2(k, h)
						h += 1
						if(h > y_axis_size)
							break()
					}
				)
			}

		}

		grid_cells_to_check
	}
	def task2(k:Int, dataset_path:String): Unit ={

		Logger.getLogger("org").setLevel(Level.WARN)
		Logger.getLogger("akka").setLevel(Level.WARN)

		val conf = new SparkConf().setMaster("local[*]").setAppName("Skyline").set("spark.executor.memory", "12g")
		val sparkSession = SparkSession.builder
			.config(conf = conf)
			.appName("Skyline")
			.getOrCreate()

		//correlated/correlated50000.csv
		//uniform/uniform1000.csv
		val df = sparkSession.read.option("header", "true").csv("uniform/uniform100.csv")
			.select(col("0").cast(DoubleType).alias("x"), col("1").cast(DoubleType).alias("y"), col("id"))

		val x_mean = df.select(avg("x")).first().getDouble(0)
		val y_mean = df.select(avg("y")).first().getDouble(0)


		val x_axis = create_grid_axis(x_mean)
		val y_axis = create_grid_axis(y_mean)

		val grid_cells_to_check = create_grid_cells_to_check(x_axis.size, y_axis.size)

		var points_to_check = 1

		for(grid_cell <-  grid_cells_to_check){

			val x_line = x_axis(grid_cell._1)
			val y_line = y_axis(grid_cell._2)

			val
			println(x_line, y_line)
		}

		exit()

		def flag_point(x_mean:Double, y_mean:Double) = udf((x:Double, y:Double) => {

			if(x < x_mean && y < y_mean)
				"dl"
			else if (x < x_mean && y > y_mean)
				"ul"
			else if (x > x_mean && y < y_mean)
				"dr"
			else
				"ur"
		})

		//    val flagged_df = df.
		//      withColumn("flag", flag_point(x_mean,y_mean)(col("x"), col("y")))

		val quartile_1 = df.filter("x >" + x_mean + " AND  y >" + y_mean) //up right
		val quartile_2 = df.filter("x <" + x_mean + " AND  y >" + y_mean) //up left
		val quartile_3 = df.filter("x <" + x_mean + " AND  y <" + y_mean) //down left
		val quartile_4 = df.filter("x >" + x_mean + " AND  y <" + y_mean) //down right

		val quartile_1_count = quartile_1.count()
		df.unpersist()
		def initialize_score(score_for_dl:Long) = udf((flag:String) => {
			if (flag == "dl")
				score_for_dl.toInt
			else
				0
		})

		//    val initialized_quartile_1 = quartile_1.withColumn("initial_score", lit(0))
		//    val initialized_quartile_2 = quartile_2.withColumn("initial_score", lit(0))
		//    val initialized_quartile_3 = quartile_3.withColumn("initial_score", lit(0))
		//    val initialized_quartile_4 = quartile_4.withColumn("initial_score", lit(0))
		//    val initial_scored_df = flagged_df.
		//      withColumn("initial_score", initialize_score(dominated_quartile_count)(col("flag")))

		//    print(initialized_quartile_1.show())
		//    print(initialized_quartile_2.show())
		//    print(initialized_quartile_3.show())
		//    print(initialized_quartile_4.show())


		val scored_quartile_2_3_1 = calculate_dominance_score(quartile_2,quartile_2.union(quartile_3).union(quartile_1),
			sparkSession)

		quartile_2.unpersist()
		val scored_quartile_4_3_1 = calculate_dominance_score(quartile_4, quartile_4.union(quartile_3).union(quartile_1),
			sparkSession)
		quartile_4.unpersist()
		val scored_quartile_1 = calculate_dominance_score_in_quartile(quartile_1, sparkSession, 0)
		quartile_1.unpersist()
		val scored_quartile_3 = calculate_dominance_score_in_quartile(quartile_3, sparkSession, quartile_1_count.toInt)
		quartile_3.unpersist()

		val scoresDf = scored_quartile_2_3_1.union(scored_quartile_4_3_1).union(scored_quartile_1).union(scored_quartile_3)
			.groupBy(col("id")).agg(sum(col("score")).alias("score"))

		scored_quartile_2_3_1.unpersist()
		scored_quartile_4_3_1.unpersist()
		scored_quartile_1.unpersist()
		scored_quartile_3.unpersist()
		//    sparkSession.time(scoresDf.sort(col("score").desc).limit(5).write.format("com.databricks.")

		print(sparkSession.time(scoresDf.sort(col("score").desc).take(k).mkString("(", ", ", ")")))
	}

	def main(args: Array[String]): Unit = {

		val k = args(0)
		val dataset_path = args(1)

		task2(k.toInt, dataset_path)
	}


}
