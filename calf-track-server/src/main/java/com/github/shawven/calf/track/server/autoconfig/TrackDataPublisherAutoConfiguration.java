package com.github.shawven.calf.track.server.autoconfig;

import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.datasource.api.DataPublisher;
import com.github.shawven.calf.track.server.publisher.DataPublisherImpl;
import com.github.shawven.calf.track.server.publisher.kafka.KafkaDataPublisher;
import com.github.shawven.calf.track.server.publisher.kafka.KafkaService;
import com.github.shawven.calf.track.server.publisher.rabbit.RabbitDataPublisher;
import com.github.shawven.calf.track.server.publisher.rabbit.RabbitService;
import com.rabbitmq.client.Channel;
import com.rabbitmq.http.client.Client;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Map;

@Configuration(proxyBeanMethods = false)
class TrackDataPublisherAutoConfiguration {

    @Bean
    @Primary
    public DataPublisherImpl dataPublisherManager(Map<String, DataPublisher> dataPublisherMap){
        return new DataPublisherImpl(dataPublisherMap);
    }


    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass({RabbitTemplate.class, Channel.class})
    static class RabbitConfiguration {

        @Bean
        public DirectExchange dataExchange(ConnectionFactory connectionFactory) {
            DirectExchange notifyExchange = new DirectExchange(Const.RABBIT_EVENT_EXCHANGE, true, false);
            new RabbitAdmin(connectionFactory).declareExchange(notifyExchange);
            return notifyExchange;
        }

        @Bean
        public Client rabbitHttpClient(RabbitProperties rabbitProperties,
                                       @Value("${spring.rabbitmq.apiUrl}") String rabbitApiUrl)  {
            try {
                return new Client(rabbitApiUrl, rabbitProperties.getUsername(), rabbitProperties.getPassword());
            } catch (MalformedURLException | URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }

        @Bean
        public RabbitService rabbitService(RabbitTemplate rabbitTemplate, Client client) {
            return new RabbitService(rabbitTemplate, client);
        }

        @Bean
        public DataPublisher rabbitDataPublisher(RabbitTemplate rabbitTemplate) {
            return new RabbitDataPublisher(rabbitTemplate);
        }
    }


    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(KafkaTemplate.class)
    static class KafkaConfiguration {

        @Bean
        public DataPublisher kafkaDataPublisher(KafkaTemplate<String, String> kafkaTemplate) {
            return new KafkaDataPublisher(kafkaTemplate);
        }

        @Bean
        public KafkaService kafkaService(KafkaProperties properties) {
            AdminClient adminClient = AdminClient.create(properties.buildAdminProperties());
            return new KafkaService(adminClient);
        }
    }
}
