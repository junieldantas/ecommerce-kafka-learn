package br.com.dantas.ecommerce;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class NewOrderMain {

    // Criando Produtor para publicar mensagem em tópico usando kafka
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(properties()); //Inicializa o producer com as configurações properties()
        var key = "ORDER_123"; //Order Number
        var value = "321,12320"; //Client Number, Valor

        var recordTopic = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", key, value); //Cria Producer para Topico ECOMMERCE_NEW_ORDER
        //send(p1, p2) - Envia para o tópico, onde p1 é o registro e p2 é um callback a ser executado ao finalizar o envio / get() aguarda o retorno
        producer.send(recordTopic, (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            log.info("[ORDER - NEW_ORDER_TOPIC] - Success - send New Order {} To Topic {}", key, data.topic());
        }).get();
    }

    //Configurações para conexão com o kafka server
    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.22.199.238:9092"); //Ip e Porta do Kafka Server
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //Serializa a chave utilizando String
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //Serializa o value utilizando String
        return properties;
    }
}
