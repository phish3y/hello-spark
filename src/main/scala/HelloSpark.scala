import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoders
import org.apache.log4j.Logger
import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.time.format.DateTimeFormatter
import scala.util.Random
import java.time.Instant
import java.time.temporal.ChronoUnit

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

object HutBatch {
  val logger = Logger.getLogger(getClass().getName())

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("hut-batch")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
      )
      .getOrCreate()

    import spark.implicits._

    val huts: Dataset[Hut] = spark.read
      .option("header", "true")
      .option("multiline", "true")
      .schema(Encoders.product[Hut].schema)
      .csv("s3a://phish3y-hello-spark/huts.csv")
      .as[Hut]

    logger.info(s"hut count: ${huts.count()}")

    val enabledHuts: Dataset[Hut] = huts
      .filter(hut => hut.status.equals("T"))
      .cache()

    logger.info(s"enabled hut count: ${enabledHuts.count()}")

    logger.info(s"WA huts: ${enabledHuts.filter(hut => hut.state.equals("Washington")).count()}")
    logger.info(s"CO huts: ${enabledHuts.filter(hut => hut.state.equals("Colorado")).count()}")
    logger.info(s"CA huts: ${enabledHuts.filter(hut => hut.state.equals("California")).count()}")
    logger.info(s"OR huts: ${enabledHuts.filter(hut => hut.state.equals("Oregon")).count()}")
    logger.info(s"WY huts: ${enabledHuts.filter(hut => hut.state.equals("Wyoming")).count()}")
    logger.info(s"MT huts: ${enabledHuts.filter(hut => hut.state.equals("Montana")).count()}")
    logger.info(s"NM huts: ${enabledHuts.filter(hut => hut.state.equals("New Mexico")).count()}")

    spark.stop()
  }
}

object HutProducer {
  val logger = Logger.getLogger(getClass().getName())

  def main(args: Array[String]): Unit = {
    val kafkaBroker = sys.env.get("KAFKA_BROKER") match {
      case Some(broker) => broker
      case None => throw new IllegalStateException("KAFKA_BROKER environment variable required")
    }

    val spark: SparkSession = SparkSession.builder
      .appName("hut-producer")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
      )
      .getOrCreate()

    import spark.implicits._

    val huts: Dataset[Hut] = spark.read
      .option("header", "true")
      .option("multiline", "true")
      .schema(Encoders.product[Hut].schema)
      .csv("s3a://phish3y-hello-spark/huts.csv")
      .as[Hut]

    val kafkaDs: Dataset[Kafka] = huts
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

    kafkaDs
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", "hello-topic")
      .save()

    spark.stop()
  }
}

object HutSubscriber {
  val logger = Logger.getLogger(getClass().getName())

  def main(args: Array[String]): Unit = {
    val kafkaBroker = sys.env.get("KAFKA_BROKER") match {
      case Some(broker) => broker
      case None => throw new IllegalStateException("KAFKA_BROKER environment variable required")
    }

    val spark: SparkSession = SparkSession.builder
      .appName("hut-subscriber")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
      )
      .getOrCreate()

    import spark.implicits._

    val eventStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", "hello-topic")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val query = eventStream.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .start()

    query.awaitTermination()

    spark.stop()
  }
}

object EventProducer {
  def main(args: Array[String]): Unit = {
    val kafkaBroker = sys.env.get("KAFKA_BROKER") match {
      case Some(broker) => broker
      case None => throw new IllegalStateException("KAFKA_BROKER environment variable required")
    }

    val users     = Array("phish3y", "frantjc", "antonia", "peach", "kiriko")
    val formatter = DateTimeFormatter.ISO_INSTANT

    def generateRandomEvent(): String = {
      val user = users(Random.nextInt(users.length))
      val now  = Instant.now()

      val timestamp = if (Random.nextBoolean()) {
        now.minus(Random.nextInt(3), ChronoUnit.MINUTES)
      } else {
        now
      }

      s"""{"user":"$user","timestamp":"${formatter.format(timestamp)}"}"""
    }

    val props = new Properties()
    props.put("bootstrap.servers", kafkaBroker)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    while (true) {
      val event  = generateRandomEvent()
      val record = new ProducerRecord[String, String]("events", null, event)
      producer.send(record)
      Thread.sleep(1000)
    }
  }
}

object EventSubscriber {
  def main(args: Array[String]): Unit = {
    val kafkaBroker = sys.env.get("KAFKA_BROKER") match {
      case Some(broker) => broker
      case None => throw new IllegalStateException("KAFKA_BROKER environment variable required")
    }

    val spark: SparkSession = SparkSession.builder
      .appName("event-subscriber")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
      )
      .getOrCreate()

    import spark.implicits._

    val eventStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", "events")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", schema_of_json("""{"user":"", "timestamp":""}""")).as("data"))
      .select($"data.user", $"data.timestamp".cast("timestamp").as("timestamp"))

    val windowedCounts = eventStream
      .withWatermark("timestamp", "4 minutes")
      .groupBy(
        window($"timestamp", "6 minutes"),
        $"user"
      )
      .count()

    val query = windowedCounts.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()

    spark.stop()
  }
}
