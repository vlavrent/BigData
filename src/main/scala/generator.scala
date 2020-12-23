import java.io.{File, PrintWriter}

import breeze.stats.distributions.MultivariateGaussian
import breeze.stats.distributions.Uniform
import breeze.linalg._

import scala.collection.mutable.ListBuffer

object generator {

  def generate_kd(numOfPoints:Int, distribution:String, dimensions:Int): Unit ={


    val directory = new File(distribution)
    if (!directory.exists) {
      directory.mkdir
    }
    val writer = new PrintWriter(new File(distribution + "/" + distribution + numOfPoints + "_" + dimensions + "d.csv"))

    for(dim <- 0 until dimensions)
      writer.write(dim + ",")
    writer.write("id\n")

    if (distribution == "correlated") {

      val var_value = 50.0

      val cov_value = 45.0

      var covariance_matrix = DenseMatrix(
        (var_value, cov_value),
        (cov_value, var_value))
      var mu = DenseVector(var_value, var_value)
      if (dimensions == 3){
        covariance_matrix = DenseMatrix(
          (var_value, cov_value, cov_value),
          (cov_value, var_value, cov_value),
          (cov_value, cov_value, var_value))
        mu = DenseVector(var_value, var_value, var_value)
      }else if (dimensions == 4){
        covariance_matrix = DenseMatrix(
          (var_value, cov_value, cov_value, cov_value),
          (cov_value, var_value, cov_value, cov_value),
          (cov_value, cov_value, var_value, cov_value),
          (cov_value, cov_value, cov_value, var_value))
        mu = DenseVector(var_value, var_value, var_value, var_value)

      }

      val mvg = MultivariateGaussian(mu, covariance_matrix)

      val generated_points = mvg.sample(numOfPoints)

      var id = 1
      for (point <- generated_points ){
        for(dim <- 0 until dimensions)
          writer.write(point(dim)  + ",")
        writer.write(id + "\n")
        id += 1
      }

    }
    else if (distribution == "uniform"){
      val un = Uniform(10, 100)

      val generated_points = DenseMatrix.rand(numOfPoints, dimensions, un)

      var index = 1
      var id = 0
      for (point <- generated_points.activeValuesIterator){
        if (index%dimensions == 0){
          writer.write(point + "," + id + "\n")
          id += 1
        }else{
          writer.write(point + ",")
        }
        index += 1
      }

    }
    writer.close()

  }

  def main(args: Array[String]): Unit = {
    generator.generate_kd(1000, "correlated", 2)
    generator.generate_kd(10000, "correlated", 2)
    generator.generate_kd(50000, "correlated", 2)
    generator.generate_kd(100000, "correlated", 2)
    generator.generate_kd(500000, "correlated", 2)
    generator.generate_kd(1000000, "correlated", 2)
    generator.generate_kd(100, "uniform", 2)
    generator.generate_kd(1000, "uniform", 2)
    generator.generate_kd(10000, "uniform", 2)
    generator.generate_kd(30000, "uniform", 2)
    generator.generate_kd(50000, "uniform", 2)
    generator.generate_kd(100000, "uniform", 2)
    generator.generate_kd(500000, "uniform", 2)
    generator.generate_kd(1000000, "uniform", 2)

    generator.generate_kd(1000, "correlated", 3)
    generator.generate_kd(10000, "correlated", 3)
    generator.generate_kd(50000, "correlated", 3)
    generator.generate_kd(100000, "correlated", 3)
    generator.generate_kd(500000, "correlated", 2)
    generator.generate_kd(1000000, "correlated", 3)
    generator.generate_kd(100, "uniform", 3)
    generator.generate_kd(1000, "uniform", 3)
    generator.generate_kd(10000, "uniform", 3)
    generator.generate_kd(30000, "uniform", 3)
    generator.generate_kd(50000, "uniform", 3)
    generator.generate_kd(100000, "uniform", 3)
    generator.generate_kd(500000, "uniform", 3)
    generator.generate_kd(1000000, "uniform", 3)

    generator.generate_kd(1000, "correlated", 4)
    generator.generate_kd(10000, "correlated", 4)
    generator.generate_kd(50000, "correlated", 4)
    generator.generate_kd(100000, "correlated", 4)
    generator.generate_kd(500000, "correlated", 4)
    generator.generate_kd(1000000, "correlated", 4)
    generator.generate_kd(100, "uniform", 4)
    generator.generate_kd(1000, "uniform", 4)
    generator.generate_kd(10000, "uniform", 4)
    generator.generate_kd(30000, "uniform", 4)
    generator.generate_kd(50000, "uniform", 4)
    generator.generate_kd(100000, "uniform", 4)
    generator.generate_kd(500000, "uniform", 4)
    generator.generate_kd(1000000, "uniform", 4)

  }
}
