package br.eng.jonnes.ecommerce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String args[]) throws ExecutionException, InterruptedException {

        var producer = new KafkaProducer<String, String>(properties());
        var key = "pedido";
        var value = "{order_number: 1, description: Product 1, price: 123.99}";
        var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, key);

        // send nao eh blocante - eh assincrono, por isso o get()
        producer.send(record, (data, e) -> {
            if(e != null) {
                e.printStackTrace();
                return;
            }
            System.out.println("Sent  " + data.topic() + " - " + " patition: " + data.partition() + " offset: " + data.offset() + " timestamp: " + data.timestamp());
        }).get();

    }

    private static Properties properties() {

        var properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}
