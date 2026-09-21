package com.jizuz.mcpserver.mq.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jizuz.mcpserver.models.mq.DocChunkVectorMsg;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Slf4j
@Service
@RequiredArgsConstructor
public class DocVectorProducer {

    private Producer producer;

    private final ObjectMapper objectMapper;

    @Value("${rocketmq.proxy-endpoint}")
    private String proxyEndpoint;

    private static final String TOPIC = "doc_chunk_vector_topic";

    @PostConstruct
    public void init() throws Exception {
        ClientConfiguration clientConfig = ClientConfiguration.newBuilder()
                .setEndpoints(proxyEndpoint)
                .build();
        ClientServiceProvider provider = ClientServiceProvider.loadService();
        producer = provider.newProducerBuilder()
                .setClientConfiguration(clientConfig)
                .setTopics(TOPIC)
                .build();
    }

    public void sendVectorMsg(DocChunkVectorMsg msg) throws Exception {
        String json = objectMapper.writeValueAsString(msg);
        Message message = ClientServiceProvider.loadService()
                .newMessageBuilder()
                .setTopic(TOPIC)
                .setTag("vector_write")
                .setBody(json.getBytes())
                .build();
        SendReceipt sendReceipt = producer.send(message);
        log.info("消息发送成功 msgId={}", sendReceipt.getMessageId().toString());
    }

    @PreDestroy
    public void close() throws Exception {
        producer.close();
    }

}
