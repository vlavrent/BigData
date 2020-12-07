import java.io.{File, PrintWriter}

import breeze.stats.distributions.MultivariateGaussian
import breeze.stats.distributions.Uniform
import breeze.linalg._

object generator {

  def generate(numOfPoints:Int, distribution:String): Unit ={


    val directory = new File(distribution)
    if (!directory.exists) {
      directory.mkdir
    }

    if (distribution == "correlated") {
      val var1 = 20.0
      val var2 = 20.0
      val cov = 18.5

      val covariance_matrix = DenseMatrix(
        (var1, cov),
        (cov, var2))

      val mu = DenseVector(var1, var2)

      val mvg = MultivariateGaussian(mu, covariance_matrix)

      val generated_points = mvg.sample(numOfPoints)

      val writer = new PrintWriter(new File(distribution + "\\" + distribution + numOfPoints + ".csv"))
      //"target\\scala-2.11\\"
      writer.write("0,1,id\n")
      var id = 1
      for (point <- generated_points ){
        writer.write(point(0) + ","+ point(1)+ "," + id + "\n")
        id += 1
      }

      writer.close()
    }
    else if (distribution == "uniform"){
      val un = Uniform(10, 40)

      val generated_points = DenseMatrix.rand(numOfPoints, 2, un)

      csvwrite(new File(distribution + "\\" + distribution + numOfPoints + ".csv") , generated_points, separator = ',')

    }


  }

  def main(args: Array[String]): Unit = {
    generator.generate(1000, "correlated")
    generator.generate(10000, "correlated")
    generator.generate(50000, "correlated")
    generator.generate(100000, "correlated")
    generator.generate(500000, "correlated")
    generator.generate(1000, "uniform")
    generator.generate(10000, "uniform")
    generator.generate(100000, "uniform")
  }
}
