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
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.SerializationFeature
import java.sql.Timestamp

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

case class GPSEvent(
    vehicle: String,
    latitude: Double,
    longitude: Double,
    speed: Double,
    heading: Double,
    timestamp: java.sql.Timestamp
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
      case None => throw new IllegalArgumentException("KAFKA_BROKER environment variable required")
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
      case None => throw new IllegalArgumentException("KAFKA_BROKER environment variable required")
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
      case None => throw new IllegalArgumentException("KAFKA_BROKER environment variable required")
    }

    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    var lat       = 37.7749
    var lon       = -122.4194
    var speed     = 50.0
    var heading   = 90.0
    val vehicleId = "car-1"

    def generateGpsEvent(): String = {
      val distanceKmPerSec = speed / 3600.0
      val headingRad       = Math.toRadians(heading)

      val deltaLat = distanceKmPerSec * Math.cos(headingRad) / 111.0
      val deltaLon =
        distanceKmPerSec * Math.sin(headingRad) / (111.0 * Math.cos(Math.toRadians(lat)))

      lat += deltaLat
      lon += deltaLon

      speed += (Random.nextDouble() - 0.5) * 2
      heading += (Random.nextDouble() - 0.5) * 5

      heading = (heading + 360) % 360

      val event = GPSEvent(
        vehicle = vehicleId,
        latitude = BigDecimal(lat).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble,
        longitude = BigDecimal(lon).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble,
        speed = BigDecimal(speed).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble,
        heading = BigDecimal(heading).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble,
        timestamp = Timestamp.from(Instant.now())
      )

      objectMapper.writeValueAsString(event)
    }

    val props = new Properties()
    props.put("bootstrap.servers", kafkaBroker)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    while (true) {
      val event  = generateGpsEvent()
      val record = new ProducerRecord[String, String]("events", null, event)
      producer.send(record)
      Thread.sleep(5000)
    }
  }
}

object EventSubscriber {
  val baseNetwork: Seq[(Double, Double)] = Seq(
    (37.77490, -122.41940),
    (37.77495, -122.41930),
    (37.77500, -122.41920),
    (37.77505, -122.41910),
    (37.77510, -122.41900),
    (37.77490, -122.41950),
    (37.77495, -122.41960),
    (37.77500, -122.41970),
    (37.77505, -122.41980),
    (37.77510, -122.41990),
    (37.77480, -122.41930),
    (37.77485, -122.41920),
    (37.77490, -122.41910),
    (37.77495, -122.41900),
    (37.77500, -122.41890),
    (37.77510, -122.41880),
    (37.77485, -122.41950),
    (37.77480, -122.41960),
    (37.77475, -122.41970),
    (37.77470, -122.41980)
  )

  def haversine(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val R    = 6371000.0
    val dLat = Math.toRadians(lat2 - lat1)
    val dLon = Math.toRadians(lon2 - lon1)
    val a = Math.pow(Math.sin(dLat / 2), 2) +
      Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
      Math.pow(Math.sin(dLon / 2), 2)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    R * c
  }

  def isNearBaseNetwork(lat: Double, lon: Double, radius: Double = 10.0): Boolean = {
    baseNetwork.exists { case (baseLat, baseLon) =>
      haversine(lat, lon, baseLat, baseLon) <= radius
    }
  }

  def main(args: Array[String]): Unit = {
    val kafkaBroker = sys.env.get("KAFKA_BROKER") match {
      case Some(broker) => broker
      case None => throw new IllegalArgumentException("KAFKA_BROKER environment variable required")
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

    val schema = Encoders.product[GPSEvent].schema
    val eventStream: Dataset[GPSEvent] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", "events")
      .load()
      .selectExpr("CAST(value AS STRING)")
      // .select(from_json($"value", schema_of_json("""{"user":"", "timestamp":""}""")).as("data"))
      // .select($"data.user", $"data.timestamp".cast("timestamp").as("timestamp"))
      .select(from_json($"value", schema).as("data"))
      .select($"data.*")
      .as[GPSEvent]

    val rawQuery = eventStream.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .queryName("raw")
      .start()

    val nearbyEvents =
      eventStream.filter(event => isNearBaseNetwork(event.latitude, event.longitude))
    val nearbyQuery = nearbyEvents.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .queryName("nearbyEvents")
      .start()

    val windowAgg = eventStream
      .withWatermark("timestamp", "5 seconds")
      .groupBy(
        window($"timestamp", "30 seconds"),
        $"vehicle"
      )
      .agg(
        count("*").as("count"),
        avg($"speed").as("avg_speed")
      )

    val aggQuery = windowAgg.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .queryName("windowAgg")
      .start()

    rawQuery.awaitTermination()
    nearbyQuery.awaitTermination()
    aggQuery.awaitTermination()

    spark.stop()
  }
}
