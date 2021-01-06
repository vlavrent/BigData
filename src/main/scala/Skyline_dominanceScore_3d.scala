import DominanceScoreUtils.dominanceScore_2d_utils.create_grid_axis
import DominanceScoreUtils.dominanceScore_3d_utils.calculate_dominance_score_3d
import SkylineDominanceScoreUtils.SkylineDominanceScore3d_utils.create_grid_cells_to_check_3d
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, max}
import org.apache.spark.sql.types.DoubleType

object Skyline_dominanceScore_3d {
	def get_top_k_dominant_3d(
														 k:Int,
														 dataset_path:String,
														 x_axis_size:Int,
														 y_axis_size:Int,
														 z_axis_size:Int,
														 skyline_dataset_path:String): Unit ={

		Logger.getLogger("org").setLevel(Level.WARN)
		Logger.getLogger("akka").setLevel(Level.WARN)

		val conf = new SparkConf()
			.setAppName("DominanceScore3d_top" + k + "_" + dataset_path + "_" + x_axis_size + "_" + y_axis_size + "_" + z_axis_size)
		//			.set("spark.scheduler.mode", "FAIR")
		val sparkSession = SparkSession.builder
			.config(conf = conf)
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


		val x_axis = create_grid_axis(x_mean, x_axis_size)
		val y_axis = create_grid_axis(y_mean, y_axis_size)
		val z_axis = create_grid_axis(z_mean, z_axis_size)

		val skyline_df = sparkSession.read.option("header", "true").csv(skyline_dataset_path)
			.select(
				col("0").cast(DoubleType).alias("x"),
				col("1").cast(DoubleType).alias("y"),
				col("2").cast(DoubleType).alias("z"),
				col("id").cast("int"))
		val skyline = skyline_df.select("x","y", "z", "id")
			.rdd.map(
			row =>(
				row.getDouble(0),
				row.getDouble(1),
				row.getDouble(2),
				row.getInt(3))).collect().toList

		val grid_cells_to_check = create_grid_cells_to_check_3d(
			df,
			x_max,
			y_max,
			z_max,
			x_axis,
			y_axis,
			z_axis,
			skyline,
			k)


		println("cells to check: " + grid_cells_to_check.size)

		import sparkSession.implicits._

		var scoresDf = Seq.empty[(String, Int)].toDF("id", "score")

		for(grid_cell <- grid_cells_to_check){

			val x_line_left = grid_cell._2._2
			val x_line_right = grid_cell._2._3
			val y_line_up = grid_cell._2._4
			val y_line_down = grid_cell._2._5
			val z_line_high = grid_cell._2._6
			val z_line_low = grid_cell._2._7


			val points_to_check_df = skyline_df.filter(
				"x <= " + x_line_right + " AND y <= " + y_line_up +  " AND z <= " + z_line_high +
				" AND " + " x > " + x_line_left + " AND  y > " + y_line_down +  " AND  z > " + z_line_low)

			val cells_to_check_dominance_df = df.filter(
				"( x <= " + x_line_right + " AND x > " + x_line_left + " AND y > " + y_line_down + " AND z > " + z_line_low + " ) " +
					" OR " + " ( y <= " + y_line_up + " AND  y > " + y_line_down +  " AND x > " + x_line_right + " AND z > " + z_line_high + " ) " +
					" OR " + " ( z <= " + z_line_high + " AND  z > " + z_line_low +  " AND x > " + x_line_right + " AND y > " + y_line_down + " ) " )

			val guarantee_dominance_score = grid_cell._3._1

			val cell_scores_df = calculate_dominance_score_3d(
				points_to_check_df,
				cells_to_check_dominance_df,
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
		val z_axis_size = args(4)
		val skyline_dataset_path = args(5)
		get_top_k_dominant_3d(k.toInt, dataset_path, x_axis_size.toInt, y_axis_size.toInt, z_axis_size.toInt, skyline_dataset_path)
	}
}
