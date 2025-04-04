import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoders

case class Hut(
  status: String, 
  system: String, 
  name: String, 
  state: String, 
  elevation: Option[Int], 
  latitude: Option[Double], 
  longitude: Option[Double]
)

object HelloSpark {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder
      .appName("hello-spark")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
      .getOrCreate()

    import spark.implicits._

    val huts: Dataset[Row] = spark
      .read
      .option("header", "true")
      .option("multiline", "true")
      // .schema(Encoders.product[Hut].schema)
      .csv("s3a://phish3y-hello-spark/huts.csv")
      .cache()
      // .as[Hut]

    System.out.println(huts.count())
    // huts.foreach(hut => System.out.println(hut.getAs[String]("name")))
    
    spark.stop()
  }
}
