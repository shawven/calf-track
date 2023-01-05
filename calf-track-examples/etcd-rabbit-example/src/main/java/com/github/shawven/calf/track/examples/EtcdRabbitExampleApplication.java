package com.github.shawven.calf.track.examples;

import com.github.shawven.calf.track.client.DataSubscribeRegistry;
import com.github.shawven.calf.track.client.rabbit.RabbitDataConsumer;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.filter.CorsFilter;

@SpringBootApplication
public class EtcdRabbitExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(EtcdRabbitExampleApplication.class, args);
    }

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Value("${databaseEventServerUrl}")
    private String serverUrl;

    @Value("${appName}")
    private String appName;

    @Bean
    public DataSubscribeRegistry dataSubscribeRegistry()  {
        //初始化订阅的实现
        return new DataSubscribeRegistry()
                .setClientId(appName)
                .setServerUrl(serverUrl)
                .setDataConsumer(new RabbitDataConsumer(rabbitTemplate))
                .syncToServer();

    }

    /**
     * @return 下面两个配置为跨域访问的配置
     */
    private CorsConfiguration buildConfig() {
        CorsConfiguration corsConfiguration = new CorsConfiguration();
        corsConfiguration.addAllowedOrigin("*");
        corsConfiguration.addAllowedHeader("*");
        corsConfiguration.addAllowedMethod("*");
        return corsConfiguration;
    }

    @Bean
    public CorsFilter corsFilter() {
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", buildConfig());
        return new CorsFilter(source);
    }
}
