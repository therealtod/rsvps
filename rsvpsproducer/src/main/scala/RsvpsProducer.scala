import java.io.FileNotFoundException
import java.net.URL
import java.util.Properties

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object RsvpsProducer {
  def main(args: Array[String]): Unit = {
    println("Starting rsvps producer...")
    val propertiesFilePath = getClass.getResource("application.properties")
    val properties: Properties = new Properties()

    if (propertiesFilePath != null) {
      val source = Source.fromURL(propertiesFilePath)
      properties.load(source.bufferedReader())
    }
    else {
      Console.err.println("Properties file cannot be loaded")
      throw new FileNotFoundException("Properties file cannot be loaded")
    }
    // meetup.com RSVP stream REST API endpoint
    val connection = new URL(properties.getProperty("meetup.rsvps.stream.endpoint")).openConnection()
    val parser = new JsonFactory(new ObjectMapper()).createParser(connection.getInputStream)


    // Set the kafka producer details in the Properties object
    val producerProperties = new Properties()
    producerProperties.put("bootstrap.servers", properties.getProperty("kafka.bootstrap.servers"))
    producerProperties.put("acks", properties.getProperty("kafka.producer.acks"))
    producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProperties.put("enable.auto.commit", properties.getProperty("kafka.producer.enable.auto.commit"))
    producerProperties.put("auto.commit.interval.ms", properties.getProperty("kafka.producer.auto.commit.interval.ms"))
    producerProperties.put("session.timeout.ms", properties.getProperty("kafka.producer.session.timeout.ms"))
    val kafka_producer_object = new KafkaProducer[String, String](producerProperties)
    while (parser.nextToken() != null) {
      val message_record = parser.readValueAsTree().toString()
      println(message_record)
      val producer_record_object = new ProducerRecord[String, String](properties.getProperty("kafka.topic.name"), message_record)
      kafka_producer_object.send(producer_record_object)
    }
    kafka_producer_object.close()
  }
}