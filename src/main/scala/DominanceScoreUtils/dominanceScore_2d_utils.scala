package DominanceScoreUtils

import breeze.linalg.max
import org.apache.spark.sql.functions.{col, collect_list, lit, size}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

object dominanceScore_2d_utils {

	/** Calculates dominance score for all points in the 1st given dataframe taking into considerations the dominated
	 *  points in the 2nd given dataframe
	 *
	 *  @param df1 1st given dataframe
	 *  @param df2 2nd given dataframe
	 *  @param sparkSession the spark session to call sql
	 *  @return a dataframe of points with their dominance score
	 */
	def calculate_dominance_score_2d(df1:DataFrame, df2:DataFrame, sparkSession: SparkSession, scoreToAdd:Int): DataFrame ={

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


	/** Creates the axis need for grid using the mean
	 *
	 *  @param axis_mean mean of the dataframe
	 *  @return a list with axis' lines
	 */
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

	/** Creates the grid cells
	 *
	 *  @param x_axis_size size of x axis
	 *  @param y_axis_size size of y axis
	 *  @return a list with cells ordered by queue the must be checked
	 */
	def create_grid_cells_to_check_2d(x_axis_size:Int, y_axis_size:Int): ListBuffer[(Int,Int)] ={
		var grid_cells_to_check = new ListBuffer[(Int, Int)]()

		for(y_point <- 0 to y_axis_size){
			var x_point = 0
			for(i <- y_point to 0 by -1){
				grid_cells_to_check.append((x_point,i))
				x_point += 1
			}
		}

		for(x_point <- 1 to x_axis_size){
			var y_point = y_axis_size
			for(i <- x_point to x_axis_size){
				grid_cells_to_check.append((i,y_point))
				y_point -= 1
			}
		}

		grid_cells_to_check
	}



	/** Get cell border lines for a given grid cell
	 *
	 *  @param x_max the max on x dimension of the dataframe, where the given cell is
	 *  @param y_max the max on y dimension of the dataframe, where the given cell is
	 *  @param grid_cell a given cell
	 *  @param x_axis a list with x axis lines
	 *  @param y_axis a list with y axis lines
	 *  @return a list with border lines
	 */
	def get_cell_borders(x_max:Double,
											 y_max:Double,
											 grid_cell:(Int, Int),
											 x_axis:List[Double],
											 y_axis:List[Double]): List[Double] ={

		var x_line_right = x_max
		if (grid_cell._1 != x_axis.size) {
			x_line_right =  x_axis(grid_cell._1)
		}
		var x_line_left = .0
		if (grid_cell._1 != 0){
			x_line_left = x_axis(grid_cell._1 - 1)
		}

		var y_line_up = y_max
		if (grid_cell._2 != y_axis.size) {
			y_line_up =  y_axis(grid_cell._2)
		}
		var y_line_down = .0
		if (grid_cell._2 != 0){
			y_line_down = y_axis(grid_cell._2 - 1)
		}

		List(x_line_left, x_line_right, y_line_up, y_line_down)
	}

}
