package com.eda.performance;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
	
	private static final Logger logger = LoggerFactory.getLogger(App.class);
	private static final String TOPIC = "topic";

	public static void main(String... args) throws InterruptedException {
		
		logger.info("Stating...");
		
		//String CONFIG_FILE = "src/main/resources/confluent_example.properties";
		//String CONFIG_FILE = "src/main/resources/eventhub_example.properties";
		//String CONFIG_FILE = "src/main/resources/confluent.properties";
		//String CONFIG_FILE = "src/main/resources/confluent_azure.properties";
		String CONFIG_FILE = "src/main/resources/eventhub_tu.properties";
		//String CONFIG_FILE = "src/main/resources/eventhub_pu.properties";
		
		final ExecutorService executorService = Executors.newFixedThreadPool(8);
		
		executorService.execute(new MyConsumer(TOPIC, CONFIG_FILE, "C:\\Tools\\data\\eda\\consumer1"));
		executorService.execute(new MyProducer(TOPIC, CONFIG_FILE));
		executorService.shutdown();
		
		logger.info("Done...");
	}
	
}
