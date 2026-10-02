package com.jizuz.mcpserver.mq.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jizuz.mcpserver.models.mq.DocChunkVectorMsg;
import com.jizuz.mcpserver.models.mq.RagDocChangeEvent;
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
import java.nio.charset.StandardCharsets;

@Slf4j
@Service
@RequiredArgsConstructor
public class DocVectorProducer {

    private Producer producer;

    private final ObjectMapper objectMapper;

    @Value("${rocketmq.proxy-endpoint}")
    private String proxyEndpoint;

    /** 统一主题：新旧链路共用，通过tag区分（vector_write=旧块级写入；TAG_ADD/TAG_UPDATE/TAG_DELETE=文档变更事件） */
    public static final String TOPIC = "doc_chunk_vector_topic";

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

    /**
     * 发送文档变更事件（doc级，权威源落库后调用）：
     * 同一topic通过tag与旧链路隔离。
     * 注意：不用setMessageGroup（FIFO消息）——存量topic为NORMAL类型，broker会拒收；
     * 同文档事件乱序由消费端md5校验兜底（旧版本事件直接丢弃，等待下一次全量重建校正）。
     */
    public void sendDocChangeEvent(RagDocChangeEvent event) throws Exception {
        String json = objectMapper.writeValueAsString(event);
        Message message = ClientServiceProvider.loadService()
                .newMessageBuilder()
                .setTopic(TOPIC)
                .setTag("TAG_" + event.getEventType())
                .setKeys(event.getMsgId())
                .setBody(json.getBytes(StandardCharsets.UTF_8))
                .build();
        SendReceipt sendReceipt = producer.send(message);
        log.info("文档变更事件发送成功 msgId={}, eventType={}, docId={}",
                sendReceipt.getMessageId(), event.getEventType(), event.getDocId());
    }

    @PreDestroy
    public void close() throws Exception {
        producer.close();
    }

}
