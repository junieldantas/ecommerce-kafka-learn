package br.com.dantas.ecommerce;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class FraudDetectorService {
    //Criando consumidor para o topico
    public static void main(String[] args) throws InterruptedException {
        var consumer = new KafkaConsumer<String, String>(properties()); //Inicializa consumer com as configirações properties()
        consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER")); //realiza subscribe no topico a ser consumido
        while (true) { //Loop para leitura do topico
            var records = consumer.poll(Duration.ofMillis(100)); //poll() realiza a leitura e recebe como parametro a duração
            if (!records.isEmpty()) {
                log.info("[ORDER - CONSUMER_ORDER_TOPIC] - {} Records Found", records.count()); //total de registros encontrados no tópico
            }
            //Imprime os registros encontrados no tópico
            for (var recordTopic : records) {
                log.info("---------------------------------");
                log.info("Processing new order, checking for fraud to key: {} value: {} ", recordTopic.key(), recordTopic.value()); //imprime dados do registro
                Thread.sleep(5000); //tempo de espera em milisegundos para imprimir proximo registro
                log.info("Order processed");
            }
        }
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.22.199.238:9092"); //Ip e Porta do kafka server
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //Deserealiza utilizando String
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //Deserealiza utilizando String
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName()); //Define group_id para consumo do topico
        //GROUP_ID_CONFIG - se mais de um serviço utilizar o mesmo group id as mensagens do topico serão distribuidas entre os consumidores de mesmo group id
        return properties;
    }
}
