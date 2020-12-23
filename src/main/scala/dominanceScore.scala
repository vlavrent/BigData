import java.lang.Thread.sleep
import java.util.regex.Pattern

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
	def calculate_dominance_score(df1:DataFrame, df2:DataFrame, sparkSession: SparkSession, scoreToAdd:Int): DataFrame ={

		//JOIN !(col("df1.flag") === "dl" && col("df2.flag") === "ur")
		val joinDF = df1.as("df1")
			.join(df2.as("df2"), (col("df1.x") < col("df2.x") &&  col("df1.y") < col("df2.y")))
			.select(col("df1.id").alias("lid"),
				col("df2.id").alias("rid"))


		val scored_df_without_zero = joinDF.groupBy(col("lid").alias("id"))
			.agg(size(collect_list("rid")).alias("counted_score"))
			.withColumn("score", lit(scoreToAdd) + col("counted_score"))
			.select(col("id"), col("score"))

		val scored_df = df1.select("id").join(scored_df_without_zero, Seq("id"), "full_outer").na.fill(scoreToAdd, Seq("score"))

		scored_df
	}

	def create_grid_axis(axis_mean:Double): List[Double] ={
		val axis_point4 = axis_mean
		val axis_point2 = axis_point4 / 2.0
		val axis_point1 = axis_point2 / 2.0
		val axis_point3 = axis_point1 + axis_point2

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

	def get_cell_borders(x_max:Double,
											 x_axis_size:Int,
											 y_max:Double,
											 y_axis_size:Int,
											 grid_cell:(Int, Int),
											 x_axis:List[Double],
											 y_axis:List[Double]): List[Double] ={

		var x_line_right = x_max
		if (grid_cell._1 != x_axis_size) {
			x_line_right =  x_axis(grid_cell._1)
		}
		var x_line_left = .0
		if (grid_cell._1 != 0){
			x_line_left = x_axis(grid_cell._1 - 1)
		}

		var y_line_up = y_max
		if (grid_cell._2 != y_axis_size) {
			y_line_up =  y_axis(grid_cell._2)
		}
		var y_line_down = .0
		if (grid_cell._2 != 0){
			y_line_down = y_axis(grid_cell._2 - 1)
		}

		List(x_line_left, x_line_right, y_line_up, y_line_down)
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
		val df = sparkSession.read.option("header", "true").csv("uniform/uniform1000000_2d.csv")
			.select(col("0").cast(DoubleType).alias("x"), col("1").cast(DoubleType).alias("y"), col("id"))

		val x_mean = df.select(avg("x")).first().getDouble(0)
		val y_mean = df.select(avg("y")).first().getDouble(0)

		val x_max = df.select(max("x")).first().getDouble(0)
		val y_max = df.select(max("y")).first().getDouble(0)

		val x_axis = create_grid_axis(x_mean)
		val y_axis = create_grid_axis(y_mean)

		val grid_cells_to_check = create_grid_cells_to_check(x_axis.size, y_axis.size)

		var cells_to_check_together = 1
		var low_diagonal = true
		var index = 0


		import sparkSession.implicits._


		var scoresDf = Seq.empty[(String, Int)].toDF("id", "score")

		breakable(
			while(scoresDf.count().toInt < 10 ){
				for (_ <- 0 until cells_to_check_together){
					//to do here the code
//					println(grid_cells_to_check(index))
					val grid_cell = grid_cells_to_check(index)
					val borders = get_cell_borders(
						x_max,
						x_axis.size,
						y_max,
						y_axis.size,
						grid_cell,
						x_axis,
						y_axis)

					val x_line_left = borders(0)
					val x_line_right = borders(1)
					val y_line_up = borders(2)
					val y_line_down = borders(3)

					val cell_dominator_df = df.filter("x <= " + x_line_right + " AND y <= " + y_line_up +
						" AND " + " x > " + x_line_left + " AND  y > " + y_line_down)

					val cells_to_check_dominance_df = df.filter(
						"( x > " + x_line_right + " AND y <= " + y_line_up + " AND y > " + y_line_down + " ) " +
							" OR " + " ( y > " + y_line_up + " AND  x <= " + x_line_right +  " AND x > " + x_line_left + " ) " )

					val guarantee_dominance_score = df.filter(
						" x > " + x_line_right + " AND y > " + y_line_up + " AND y > " + y_line_down ).count().toInt

					val cell_scores_df = calculate_dominance_score(
						cell_dominator_df,
						cells_to_check_dominance_df.union(cell_dominator_df),
						sparkSession,
						guarantee_dominance_score
					)

					scoresDf = scoresDf.union(cell_scores_df)
					index += 1
				}

				if(low_diagonal)
					cells_to_check_together += 1
				else
					cells_to_check_together -= 1

				if(cells_to_check_together == x_axis.size + 1)
					low_diagonal = false

				if(index == grid_cells_to_check.size)
					break()
				println("===========")
			}
		)

		println(scoresDf.sort(col("score").desc).show(10))
		exit()

		//    sparkSession.time(scoresDf.sort(col("score").desc).limit(5).write.format("com.databricks.")

//		print(sparkSession.time(scoresDf.sort(col("score").desc).take(k).mkString("(", ", ", ")")))/\
	}

	def main(args: Array[String]): Unit = {

		val k = args(0)
		val dataset_path = args(1)

		task2(k.toInt, dataset_path)
	}


}
