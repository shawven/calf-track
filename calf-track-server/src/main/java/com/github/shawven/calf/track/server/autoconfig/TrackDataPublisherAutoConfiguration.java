package com.github.shawven.calf.track.server.autoconfig;

import com.github.shawven.calf.track.common.Const;
import com.github.shawven.calf.track.datasource.api.DataPublisher;
import com.github.shawven.calf.track.server.publisher.rabbit.RabbitDataPublisher;
import com.github.shawven.calf.track.server.publisher.rabbit.RabbitService;
import com.rabbitmq.http.client.Client;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.MalformedURLException;
import java.net.URISyntaxException;

@Configuration(proxyBeanMethods = false)
class TrackDataPublisherAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(ConnectionFactory.class)
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


//    @Configuration(proxyBeanMethods = false)
//    @ConfigurationProperties(prefix = "kafka.zk")
//    @EnableConfigurationProperties(KafkaProperties.class)
//    static class KafkaConfiguration {
//
//        @Autowired
//        private KafkaProperties kafkaProperties;
//
//        @Bean
//        public ZkClient zkClient() {
//            return new ZkClient(kafkaProperties.getServers(),
//                    kafkaProperties.getSessionTimeout(),
//                    kafkaProperties.getConnectionTimeout(),
//                    ZKStringSerializer$.MODULE$);
//        }
//    }
}
