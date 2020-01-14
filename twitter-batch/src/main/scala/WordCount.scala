import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, col, count}
// sbt compile
// sbt package

//before run export keys
//export AWS_ACCESS_KEY_ID=$(aws --profile personal configure get aws_access_key_id)
//export AWS_SECRET_ACCESS_KEY=$(aws --profile personal configure get aws_secret_access_key)

//spark-submit --class WordCount --packages org.apache.hadoop:hadoop-aws:2.7.0 --master local target/scala-2.11/twitter-batch_2.11-1.0.jar


object WordCount {

  val s3Path = "s3a://kinesisscoynetest/2020/01/03/21/kinesis-skill-building-1-2020-01-03-21-13-44-ada04474-6683-40b3-b177-cd56e030a9eb"

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("WordCount")
      .enableHiveSupport()
      .getOrCreate()

    println("Starting spark")

    val json = spark.read.format("json").load(s3Path)

    val cities = json.where(col("place.place_type") === lit("city"))
                     .groupBy(col("place.name"))
                     .agg(count(col("place.name")).as("count"))
    cities.show()

  }

}