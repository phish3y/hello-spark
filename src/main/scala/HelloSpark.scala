import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoders
import org.apache.log4j.Logger

case class Hut(
  status: String, 
  system: String, 
  name: String, 
  state: String, 
  elevation: Option[Int], 
  latitude: Option[Double], 
  longitude: Option[Double],
  hut_url: Option[String],
  booking_url: Option[String],
  short_description: Option[String],
  summary: Option[String],
  mileage_one_way: Option[Double],
  elevation_gain: Option[Int],
  capacity: Option[Int],
  dogs: Option[String], 
  snowmachine: Option[String],
  booking_notes: Option[String]
)

case class Kafka(
  key: Option[String],
  value: String,
  headers: Option[Array[String]],
  topic: Option[String],
  partition: Option[Int]
)

object HelloSparkBatch {
  val logger = Logger.getLogger(getClass().getName())

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder
      .appName("hello-spark-batch")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
      .getOrCreate()

    import spark.implicits._

    val huts: Dataset[Hut] = spark
      .read
      .option("header", "true")
      .option("multiline", "true")
      .schema(Encoders.product[Hut].schema)
      .csv("s3a://phish3y-hello-spark/huts.csv")
      .as[Hut]

    val hut_count = huts.count()
    logger.info(s"hut count: ${hut_count}")
    
    val enabled_huts: Dataset[Hut] = huts
      .filter(hut => hut.status.equals("T"))
      .cache()

    logger.info(s"enabled hut count: ${enabled_huts.count()}")

    logger.info(s"WA huts: ${enabled_huts.filter(hut => hut.state.equals("Washington")).count()}")
    logger.info(s"CO huts: ${enabled_huts.filter(hut => hut.state.equals("Colorado")).count()}")
    logger.info(s"CA huts: ${enabled_huts.filter(hut => hut.state.equals("California")).count()}")
    logger.info(s"OR huts: ${enabled_huts.filter(hut => hut.state.equals("Oregon")).count()}")
    logger.info(s"WY huts: ${enabled_huts.filter(hut => hut.state.equals("Wyoming")).count()}")
    logger.info(s"MT huts: ${enabled_huts.filter(hut => hut.state.equals("Montana")).count()}")
    logger.info(s"NM huts: ${enabled_huts.filter(hut => hut.state.equals("New Mexico")).count()}")
   
    spark.stop()
  }
}

object HelloSparkStreamProducer {
  val logger = Logger.getLogger(getClass().getName())

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder
      .appName("hello-spark-batch")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
      .getOrCreate()

    import spark.implicits._

    val huts: Dataset[Hut] = spark
      .read
      .option("header", "true")
      .option("multiline", "true")
      .schema(Encoders.product[Hut].schema)
      .csv("s3a://phish3y-hello-spark/huts.csv")
      .as[Hut]

    val kafka_df: Dataset[Kafka] = huts
      .filter(row => row.state.equals("New Mexico")) // TODO
      .map(row => {
        val value = Seq(
          row.status,
          row.system,
          row.name,
          row.state
        ).mkString("|")

        Kafka(
          key = None,
          value = value,
          headers = None,
          topic = Some("hello-topic"),
          partition = Some(0)
        )
      })

    kafka_df
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.0.132:9094")
      .option("topic", "hello-topic")
      .save()

    spark.stop()
  }
}

object HelloSparkStreamSubscriber {
  val logger = Logger.getLogger(getClass().getName())

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder
      .appName("hello-spark-stream")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
      .getOrCreate()

    import spark.implicits._

    val stream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.0.132:9094")
      .option("subscribe", "hello-topic")
      .option("startingOffsets", "latest")
      .load()

    val messages = stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    val query = messages.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .start()

    query.awaitTermination()

    spark.stop()
  }
}