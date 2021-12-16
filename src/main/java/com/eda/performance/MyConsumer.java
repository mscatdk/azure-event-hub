package com.eda.performance;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.identity.ClientSecretCredentialBuilder;
import com.eventhub.kafka.schema.Customer;

public class MyConsumer implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(MyConsumer.class);
	private final String TOPIC;
	private String filename;
	private String CONFIG_FILE;

	public MyConsumer(String TOPIC, String CONFIG_FILE, String filename) {
		super();
		this.TOPIC = TOPIC;
		this.filename = filename;
		this.CONFIG_FILE = CONFIG_FILE;
	}

	public void run() {
		logger.info("Starting consumer...");
		TimeKeeper appStart = new TimeKeeper();
		final Consumer<Long, Customer> consumer = createConsumer();
		try {
			ConsumerRecords<Long, Customer> consumerRecords;
			BufferedWriter messageTimeWritter = new BufferedWriter(new FileWriter(new File(filename + ".txt")));
			BufferedWriter pollTimeWritter = new BufferedWriter(new FileWriter(new File(filename + "_poll.txt")));

			do {
				TimeKeeper pollTime = new TimeKeeper();
				consumerRecords = consumer.poll(Duration.ofSeconds(12));
				pollTimeWritter.write(pollTime.stop() + "," + consumerRecords.count());
				pollTimeWritter.newLine();
				for (ConsumerRecord<Long, Customer> cr : consumerRecords) {
					long diff = System.currentTimeMillis() - cr.value().getTime();
					messageTimeWritter.write(Long.toString(diff));
					messageTimeWritter.newLine();
					logger.info("{}", diff);
				}
				consumer.commitAsync();
			} while (consumerRecords.count() != 0);
			messageTimeWritter.close();
			pollTimeWritter.close();
		} catch (CommitFailedException e) {
			logger.error("CommitFailedException", e);
		} catch (IOException e) {
			logger.error("IO error", e);
		} finally {
			consumer.close();
		}
		logger.info("Consumer Done - time: {}", appStart.stop());
	}

	private Consumer<Long, Customer> createConsumer() {
		try {
			final Properties properties = new Properties();
			properties.put(KafkaAvroDeserializerConfig.AVRO_SPECIFIC_READER_CONFIG, true);
			properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
			properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

			// Get remaining properties from config file
			properties.load(new FileReader(CONFIG_FILE));
			properties.put(AbstractKafkaSerdeConfig.SCHEMA_REGISTRY_CREDENTIAL_CONFIG, new ClientSecretCredentialBuilder().tenantId(properties.getProperty("schema.group.tenantid")).clientId(properties.getProperty("schema.group.clientid")).clientSecret(properties.getProperty("schema.group.clientsecret")).build());			
			
			final Consumer<Long, Customer> consumer = new KafkaConsumer<>(properties);

			consumer.subscribe(Collections.singletonList(TOPIC));
			return consumer;

		} catch (FileNotFoundException e) {
			System.out.println("FileNotFoundException: " + e);
			System.exit(1);
			return null; // unreachable
		} catch (IOException e) {
			System.out.println("IOException: " + e);
			System.exit(1);
			return null; // unreachable
		}
	}

}
