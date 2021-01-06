import DominanceScoreUtils.dominanceScore_2d_utils.{calculate_dominance_score_2d, create_grid_axis}
import SkylineDominanceScoreUtils.SkylineDominanceScore2d_utils.create_grid_cells_to_check_2d
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, max}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.rdd.RDD

object Skyline_dominanceScore_2d {

	def get_top_k_dominant_2d(k:Int, dataset_path:String, x_axis_size:Int, y_axis_size:Int, skyline_dataset_path:String): Unit ={

		Logger.getLogger("org").setLevel(Level.WARN)
		Logger.getLogger("akka").setLevel(Level.WARN)

		val conf = new SparkConf()
			.setAppName("SkylineDominanceScore2d_top" + k + "_" + dataset_path + "_" + x_axis_size + "_" + y_axis_size)
		//			.set("spark.scheduler.mode", "FAIR")
		val sparkSession = SparkSession.builder
			.config(conf = conf)
			.getOrCreate()

		//correlated/correlated50000.csv
		//uniform/uniform1000.csv
		val df = sparkSession.read.option("header", "true").csv(dataset_path)
			.select(col("0").cast(DoubleType).alias("x"), col("1").cast(DoubleType).alias("y"), col("id"))

		val x_mean = df.select(avg("x")).first().getDouble(0)
		val y_mean = df.select(avg("y")).first().getDouble(0)

		val x_max = df.select(max("x")).first().getDouble(0)
		val y_max = df.select(max("y")).first().getDouble(0)

		val x_axis = create_grid_axis(x_mean, x_axis_size)
		val y_axis = create_grid_axis(y_mean, y_axis_size)

		// code to get skyline
		val sky = sparkSession.read.option("header", "true").csv(dataset_path)
			.select(col("0").cast(DoubleType).alias("x"), col("1").cast(DoubleType).alias("y"), col("id"))
		val skyline = sky.select("x","y","id").rdd.map(x=>(x.getDouble(0),x.getDouble(1),x.getInt(2))).collect().toList


		//val skyline = List((10.50646332, 10.132206, 457),
			//(10.37956577, 11.12590036, 94500),
			//(13.12073157, 10.00053383, 5174),
			//(10.056831, 15.46942073, 9379),
			//(10.00818797, 22.65472621, 4884))


		val grid_cells_to_check = create_grid_cells_to_check_2d(
			df,
			x_max,
			y_max,
			x_axis,
			y_axis,
			skyline,
			k)

		println("cells to check: " + grid_cells_to_check.size)

		var points_checked = 0
		import sparkSession.implicits._

		var scoresDf = Seq.empty[(String, Int)].toDF("id", "score")

		val skyline_df = sparkSession.createDataFrame(skyline)
			.select(
				col("_1").cast(DoubleType).alias("x"),
				col("_2").cast(DoubleType).alias("y"),
				col("_3").alias("id"))
		for(grid_cell <- grid_cells_to_check){

			val x_line_left = grid_cell._2._2
			val x_line_right = grid_cell._2._3
			val y_line_up = grid_cell._2._4
			val y_line_down = grid_cell._2._5

			val cell_dominator_df = df.filter("x <= " + x_line_right + " AND y <= " + y_line_up +
				" AND " + " x > " + x_line_left + " AND  y > " + y_line_down)

			val points_to_check_df = skyline_df.filter("x <= " + x_line_right + " AND y <= " + y_line_up +
				" AND " + " x > " + x_line_left + " AND  y > " + y_line_down)

			val cells_to_check_dominance_df = df.filter(
				"( x > " + x_line_right + " AND y <= " + y_line_up + " AND y > " + y_line_down + " ) " +
					" OR " + " ( y > " + y_line_up + " AND  x <= " + x_line_right +  " AND x > " + x_line_left + " ) " )

			val guarantee_dominance_score = grid_cell._3._1

			val cell_scores_df = calculate_dominance_score_2d(
				points_to_check_df,
				cells_to_check_dominance_df.union(cell_dominator_df),
				sparkSession,
				guarantee_dominance_score)

			scoresDf = scoresDf.union(cell_scores_df)

		}

		println(scoresDf.sort(col("score").desc).show(k))
	}

	def main(args: Array[String]): Unit = {

		val k = args(0)
		val dataset_path = args(1)
		val x_axis_size = args(2)
		val y_axis_size = args(3)
		val skyline_dataset_path = args(4)
		get_top_k_dominant_2d(k.toInt, dataset_path, x_axis_size.toInt, y_axis_size.toInt,skyline_dataset_path)
	}
}
