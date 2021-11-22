package com.example.demo1.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author tianzhuang
 * @version 1.0
 * @date 2021/11/17 18:48
 */
@Configuration
@ConfigurationProperties(value = "elasticsearch")
public class ElasticSearchConfig {
    private String host;
    private String port;

    @Bean
    public RestHighLevelClient esRestClient(){
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        //在这里配置你的elasticsearch的情况
                        new HttpHost("127.0.0.1", 9200, "http")
                )
        );
        return client;
    }
}
