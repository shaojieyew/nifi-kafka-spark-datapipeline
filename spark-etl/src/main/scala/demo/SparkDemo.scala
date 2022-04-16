package demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, max}

object SparkDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").getOrCreate()

    import spark.implicits._

    val data = Seq(
      (1, "john", 1000, 3),
      (2, "mary", 14000, 1),
      (3, "tommy", 4500, 3),
      (4, "dickson", 3000, 2),
      (5, "samuel", 4000, 1)
    ).toDF("id", "name", "salary", "department")
    data.show()

    val department = Seq(
      (1, "IT Department"),
      (2, "Marketing"),
      (3, "Human Resource"))
      .toDF("id", "name")
    department.show()

    data.where(col("salary") >= 4000).show

    data.agg(avg("salary"), max("salary")).show

    data.join(department, data("department") === department("id"), "left_outer").show

    while (true) {
      // to keep Spark UI alive
      // visit localhost:4040 in browser
    }

  }
}
