package com.eda.performance;

import java.io.FileReader;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.identity.ClientSecretCredentialBuilder;
import com.eventhub.kafka.schema.Customer;


public class MyProducer implements Runnable {
	
	private static final Logger logger = LoggerFactory.getLogger(MyProducer.class);
	private String TOPIC;
	private String CONFIG_FILE;

	public MyProducer(String TOPIC, String CONFIG_FILE) {
		super();
		this.TOPIC = TOPIC;
		this.CONFIG_FILE = CONFIG_FILE;
	}	

	public void run() {
		logger.info("Starting producer...");
		TimeKeeper appStart = new TimeKeeper();
		final Producer<Long, Customer> producer = createProducer();
		//producer.initTransactions();
		
		for (int i=0; i<10000; i++) {
			long time = System.currentTimeMillis();
			final ProducerRecord<Long, Customer> record = new ProducerRecord<Long, Customer>(TOPIC, time, Customer.newBuilder().setAge(20).setName("test").setId("rererer").setAddress("dfgsdfg").setNickname("Test").setTime(time).build());
            //producer.beginTransaction();
			producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                    	logger.error("Error", exception);
                        System.exit(1);
                    }
                }
            });			
			//producer.commitTransaction();
		}
		producer.flush();
		producer.close();
		logger.info("Producer Done - " + appStart.stop());
	}
	
    private Producer<Long, Customer> createProducer() {
        try{
            Properties properties = new Properties();
            properties.load(new FileReader(CONFIG_FILE));
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
            //properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            //properties.put(ProducerConfig.CLIENT_ID_CONFIG, "transactional-producer");
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
            //properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-run-now");
            properties.put(AbstractKafkaSerdeConfig.SCHEMA_REGISTRY_CREDENTIAL_CONFIG, new ClientSecretCredentialBuilder().tenantId(properties.getProperty("schema.group.tenantid")).clientId(properties.getProperty("schema.group.clientid")).clientSecret(properties.getProperty("schema.group.clientsecret")).build());

            return new KafkaProducer<>(properties);
        } catch (Exception e){
            System.out.println("Failed to create producer with exception: " + e);
            e.printStackTrace();
            System.exit(0);
            return null;        //unreachable
        }
    }

}
