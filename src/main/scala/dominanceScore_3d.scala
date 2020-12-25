import DominanceScoreUtils.dominanceScore_2d_utils.create_grid_axis
import DominanceScoreUtils.dominanceScore_3d_utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, max, var_pop}
import org.apache.spark.sql.types.DoubleType

import scala.util.control.Breaks._

object dominanceScore_3d {

	def get_top_k_dominant_3d(k:Int, dataset_path:String): Unit ={

		Logger.getLogger("org").setLevel(Level.WARN)
		Logger.getLogger("akka").setLevel(Level.WARN)

		val conf = new SparkConf().setMaster("local[*]").setAppName("Skyline").set("spark.executor.memory", "12g")
		val sparkSession = SparkSession.builder
			.config(conf = conf)
			.appName("Skyline")
			.getOrCreate()

		//correlated/correlated50000.csv
		//uniform/uniform1000.csv
		val df = sparkSession.read.option("header", "true").csv(dataset_path)
			.select(
				col("0").cast(DoubleType).alias("x"),
				col("1").cast(DoubleType).alias("y"),
				col("2").cast(DoubleType).alias("z"), col("id"))

		val x_mean = df.select(avg("x")).first().getDouble(0)
		val y_mean = df.select(avg("y")).first().getDouble(0)
		val z_mean = df.select(avg("z")).first().getDouble(0)

		val x_max = df.select(max("x")).first().getDouble(0)
		val y_max = df.select(max("y")).first().getDouble(0)
		val z_max = df.select(max("z")).first().getDouble(0)


		val x_axis = create_grid_axis(x_mean)
		val y_axis = create_grid_axis(y_mean)
		val z_axis = create_grid_axis(z_mean)

		val grid_cells_to_check = create_grid_cells_to_check_3d(
			df,
			x_max,
			y_max,
			z_max,
			x_axis,
			y_axis,
			z_axis,
			k)

		println("cells to check: " + grid_cells_to_check.size)
		println("points to check: " + grid_cells_to_check.map(_._2._1).sum)

		var points_checked = 0
		import sparkSession.implicits._

		var scoresDf = Seq.empty[(String, Int)].toDF("id", "score")

		for(grid_cell <- grid_cells_to_check){

			val x_line_left = grid_cell._2._2
			val x_line_right = grid_cell._2._3
			val y_line_up = grid_cell._2._4
			val y_line_down = grid_cell._2._5
			val z_line_high = grid_cell._2._6
			val z_line_low = grid_cell._2._7

			val cell_dominator_df = df.filter(
				"x <= " + x_line_right + " AND y <= " + y_line_up +  " AND z <= " + z_line_high +
					" AND " + " x > " + x_line_left + " AND  y > " + y_line_down +  " AND  z > " + z_line_low)

			val cells_to_check_dominance_df = df.filter(
				"( x > " + x_line_right + " AND y <= " + y_line_up + " AND y > " + y_line_down + " AND z <= " + z_line_high + " AND z > " + z_line_low + " ) " +
					" OR " + " ( y > " + y_line_up + " AND  x <= " + x_line_right +  " AND x > " + x_line_left + " AND z <= " + z_line_high + " AND z > " + z_line_low + " ) " +
					" OR " + " ( z > " + z_line_high + " AND  x <= " + x_line_right +  " AND x > " + x_line_left + " AND y <= " + y_line_up + " AND y > " + y_line_down +" ) " )

			val guarantee_dominance_score = grid_cell._3._1

			val cell_scores_df = calculate_dominance_score_3d(
				cell_dominator_df,
				cells_to_check_dominance_df.union(cell_dominator_df),
				sparkSession,
				guarantee_dominance_score)

			scoresDf = scoresDf.union(cell_scores_df)

		}

		// time counting needs fixing
		println(sparkSession.time(scoresDf.sort(col("score").desc).show(k)))
	}

	def main(args: Array[String]): Unit = {

		val k = args(0)
		val dataset_path = args(1)

		get_top_k_dominant_3d(k.toInt, dataset_path)
	}



}
