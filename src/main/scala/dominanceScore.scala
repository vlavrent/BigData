import DominanceScoreUtils.dominanceScore_2d_utils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{avg, col, max}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.SparkSession
import scala.util.control.Breaks._

object dominanceScore {

	def get_top_k_dominant_2d(k:Int, dataset_path:String): Unit ={

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
			while(scoresDf.count().toInt < k ){
				for (_ <- 0 until cells_to_check_together){

					val grid_cell = grid_cells_to_check(index)
					val borders = get_cell_borders(
						x_max,
						y_max,
						grid_cell,
						x_axis,
						y_axis)

					val x_line_left = borders(0)
					val x_line_right = borders(1)
					val y_line_up = borders(2)
					val y_line_down = borders(3)

					val cell_dominator_df = df.filter("x <= " + x_line_right + " AND y <= " + y_line_up +
						" AND " + " x > " + x_line_left + " AND  y > " + y_line_down)
					if (cell_dominator_df.count() != 0){

						val cells_to_check_dominance_df = df.filter(
							"( x > " + x_line_right + " AND y <= " + y_line_up + " AND y > " + y_line_down + " ) " +
								" OR " + " ( y > " + y_line_up + " AND  x <= " + x_line_right +  " AND x > " + x_line_left + " ) " )

						val guarantee_dominance_score = df.filter(
							" x > " + x_line_right + " AND y > " + y_line_up + " AND y > " + y_line_down ).count().toInt

						val cell_scores_df = calculate_dominance_score_2d(
							cell_dominator_df,
							cells_to_check_dominance_df.union(cell_dominator_df),
							sparkSession,
							guarantee_dominance_score)

						scoresDf = scoresDf.union(cell_scores_df)
					}

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
			}
		)
		// time counting needs fixing
		println(sparkSession.time(scoresDf.sort(col("score").desc).show(k)))
	}

	def main(args: Array[String]): Unit = {

		val k = args(0)
		val dataset_path = args(1)

		get_top_k_dominant_2d(k.toInt, dataset_path)
	}


}
