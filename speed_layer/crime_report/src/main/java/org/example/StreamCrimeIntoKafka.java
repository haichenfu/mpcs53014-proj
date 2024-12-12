package org.example;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.GenericType;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.glassfish.jersey.jackson.JacksonFeature;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


// Inspired by http://stackoverflow.com/questions/14458450/what-to-use-instead-of-org-jboss-resteasy-client-clientrequest
public class StreamCrimeIntoKafka {
	static class Task extends TimerTask {
		private Client client;
		Random generator = new Random();
		// We are just going to get a random sampling of flights from a few airlines
		// Getting all flights would be much more expensive!

		public List<ObservedCrime> getObservedCrime() {
				// Get the current time truncated to seconds
				String now = Instant.now().truncatedTo(ChronoUnit.SECONDS).toString().replace("Z", "");

				// Get the time 3 minutes ago
				String threeMinutesAgo = Instant.now().minus(60, ChronoUnit.MINUTES).truncatedTo(ChronoUnit.SECONDS).toString().replace("Z", "");

				// Construct the $where clause
				String whereClause = String.format("date between '%s' and '%s'", threeMinutesAgo, now);


				Invocation.Builder bldr = client.target("https://data.cityofchicago.org/resource/ijzp-q8t2.json")
						.queryParam("$select", "id,date,primary_type,location_description") // Fields to select
						.queryParam("$where", whereClause) // Filter records within the last 3 minutes
						.request("application/json")
						.header("X-App-Token", "JgumTtgr5cQQx47hIHJohQHoH");
				try {
					List<ObservedCrime> observedCrimes = bldr.get(new GenericType<List<ObservedCrime>>() {});
					System.out.println(observedCrimes.size());
					return observedCrimes;
				} catch (Exception e) {
					System.err.println(e.getMessage());
				}
				return null;
		}


		// Adapted from http://hortonworks.com/hadoop-tutorial/simulating-transporting-realtime-events-stream-apache-kafka/
		Properties props = new Properties();
		String TOPIC = "haichenfu-crime-report";
		KafkaProducer<String, String> producer;
		
		public Task() {
			client = ClientBuilder.newClient();
			// enable POJO mapping using Jackson - see
			// https://jersey.java.net/documentation/latest/user-guide.html#json.jackson
			client.register(JacksonFeature.class); 
			props.put("bootstrap.servers", bootstrapServers);
			props.put("acks", "all");
			props.put("retries", 0);
			props.put("batch.size", 16384);
			props.put("linger.ms", 1);
			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			producer = new KafkaProducer<>(props);
		}

		@Override
		public void run() {

			List<ObservedCrime> observedCrimes = getObservedCrime();
			if(observedCrimes == null) {
				System.out.println("retrieved result is null");
				return;
			}
			ObjectMapper mapper = new ObjectMapper();
			System.out.println("running");
			for(ObservedCrime observation : observedCrimes) {
				System.out.println("hi");
				System.out.println(observation.getPrimary_type());
				ProducerRecord<String, String> data;
				try {
					KafkaCrimeRecord kfr = new KafkaCrimeRecord(
							observation.getId(),
							observation.getDate(),
							observation.getPrimary_type(),
							observation.getLocation_description()
					);
					System.out.println(kfr.getId());
					data = new ProducerRecord<String, String>
					(TOPIC, 
					 mapper.writeValueAsString(kfr));
					producer.send(data,(recordMetadata, exception) -> {
						if (exception == null) {
							System.out.println("Record written to offset " +
									recordMetadata.offset() + " timestamp " +
									recordMetadata.timestamp());
						} else {
							System.err.println("An error occurred");
							exception.printStackTrace(System.err);
						}
					});
				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}

	static String bootstrapServers = new String("localhost:9092");

	public static void main(String[] args) {
		if(args.length > 0)  // This lets us run on the cluster with a different kafka
			bootstrapServers = args[0];
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new Task(), 0, 60*60*1000);
	}
}

