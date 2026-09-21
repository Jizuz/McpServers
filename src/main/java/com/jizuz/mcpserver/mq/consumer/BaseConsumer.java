package com.jizuz.mcpserver.mq.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageView;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public abstract class BaseConsumer<T> {

    protected SimpleConsumer simpleConsumer;
    protected final ObjectMapper objectMapper;
    private static final ClientServiceProvider PROVIDER = ClientServiceProvider.loadService();
    private ExecutorService pullExecutor;
    private volatile boolean running = false;

    // 消息不可见时间：拿到消息后，多少秒内不ack，broker自动重试
    private static final Duration INVISIBLE_DURATION = Duration.ofSeconds(30);

    protected BaseConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public void init() throws ClientException {
        ClientConfiguration clientConfig = ClientConfiguration.newBuilder()
                .setEndpoints(getProxyEndpoint())
                .build();

        FilterExpression filterExpression;
        String tagExpr = getTagExpression();
        if ("*".equals(tagExpr)) {
            filterExpression = FilterExpression.SUB_ALL;
        } else {
            filterExpression = new FilterExpression(tagExpr);
        }

        // 注意：订阅关系必须在 build() 之前通过 setSubscriptionExpressions 设置，
        // build() 会校验 subscriptionExpressions（rocketmq-client-java 5.x API 要求）
        simpleConsumer = PROVIDER.newSimpleConsumerBuilder()
                .setClientConfiguration(clientConfig)
                .setConsumerGroup(getConsumerGroup())
                .setSubscriptionExpressions(Collections.singletonMap(getTopic(), filterExpression))
                .setAwaitDuration(INVISIBLE_DURATION)
                .build();

        running = true;
        pullExecutor = Executors.newSingleThreadExecutor();
        pullExecutor.submit(this::pullLoop);

        log.info("[RocketMQ SimpleConsumer Init] group={}, topic={}, tag={}, proxy={}, batchNum={}",
                getConsumerGroup(), getTopic(), tagExpr, getProxyEndpoint(), getBatchMaxMessageNum());
    }

    /**
     * 拉取循环
     */
    private void pullLoop() {
        final int batchNum = getBatchMaxMessageNum();
        while (running) {
            try {
                // receive(批量条数, 消息不可见时间)
                List<MessageView> messageViews = simpleConsumer.receive(batchNum, INVISIBLE_DURATION);
                if (messageViews.isEmpty()) {
                    continue;
                }
                for (MessageView msgView : messageViews) {
                    handleMessage(msgView);
                }
            } catch (ClientException e) {
                log.error("[RocketMQ Pull ClientException]", e);
            } catch (Exception e) {
                log.error("[RocketMQ Pull Unknown Exception]", e);
            }
        }
    }

    /**
     * 处理单条消息
     */
    private void handleMessage(MessageView msgView) {
        final String msgId = msgView.getMessageId().toString();
        final String topic = msgView.getTopic();
        final String tag = msgView.getTag().orElse(null);
        int deliveryAttempt = msgView.getDeliveryAttempt();
        int retryTimes = deliveryAttempt - 1;
        final int maxRetry = getMaxRetryTimes();

        try {
            // 前置过滤
            if (!preFilter(msgView)) {
                log.info("[RocketMQ Skip Msg] msgId={}, topic={}, tag={}", msgId, topic, tag);
                ackMessage(msgView);
                return;
            }

            // 幂等校验
            if (idempotentCheck(msgView)) {
                log.info("[RocketMQ Idempotent Skip] msgId={}", msgId);
                ackMessage(msgView);
                return;
            }

            // 消息体解析
            T message = parseMessage(msgView);
            // 执行业务消费
            consume(message, msgView);
            // 幂等标记
            markIdempotent(msgView);

            if (isPrintBodyLog()) {
                log.info("[RocketMQ Consume Success] topic={}, msgId={}, deliveryAttempt={}, body={}",
                        topic, msgId, deliveryAttempt, message);
            } else {
                log.info("[RocketMQ Consume Success] topic={}, msgId={}, deliveryAttempt={}",
                        topic, msgId, deliveryAttempt);
            }
            onConsumeSuccess(msgView);
            ackMessage(msgView);
        } catch (SkipMessageException e) {
            log.warn("[RocketMQ Skip Business] msgId={}, reason={}", msgId, e.getMessage());
            ackMessage(msgView);
        } catch (Exception e) {
            log.error("[RocketMQ Consume Fail] topic={}, msgId={}, retry={}/{}",
                    topic, msgId, retryTimes, maxRetry, e);
            onConsumeFail(msgView, e);

            if (retryTimes >= maxRetry) {
                // 超过最大重试，ack，丢弃消息（死信）
                log.warn("[RocketMQ DeadLetter] msgId={}, exceed max retry {}, deliveryAttempt={}",
                        msgId, maxRetry, deliveryAttempt);
                handleDeadLetter(msgView);
                ackMessage(msgView);
            } else {
                // ❗不调用ack！等待INVISIBLE_DURATION超时，broker自动重试这条消息
                log.info("[RocketMQ Will Retry Later] msgId={}, wait invisible timeout", msgId);
            }
        }
    }

    /**
     * 消息解析
     */
    protected T parseMessage(MessageView msgView) throws Exception {
        ByteBuffer bodyBuffer = msgView.getBody();
        byte[] bytes = new byte[bodyBuffer.remaining()];
        bodyBuffer.get(bytes);
        String bodyStr = new String(bytes, StandardCharsets.UTF_8);

        TypeReference<T> typeRef = getTypeReference();
        Type type = typeRef.getType();

        if (String.class.equals(type)) {
            @SuppressWarnings("unchecked")
            T val = (T) bodyStr;
            return val;
        }
        if (byte[].class.equals(type)) {
            return (T) bytes;
        }
        return objectMapper.readValue(bodyStr, typeRef);
    }


    /**
     * 封装ack，捕获ClientException
     */
    private void ackMessage(MessageView msgView) {
        try {
            simpleConsumer.ack(msgView);
        } catch (ClientException e) {
            log.error("[RocketMQ Ack Fail] msgId={}", msgView.getMessageId(), e);
        }
    }

    // ========== 子类必须实现 ==========
    protected abstract TypeReference<T> getTypeReference();
    protected abstract void consume(T message, MessageView msgView);
    protected abstract String getTopic();
    protected abstract String getConsumerGroup();
    protected abstract String getProxyEndpoint();

    // ========== 可选重写 ==========
    protected String getTagExpression() {
        return "*";
    }

    protected int getBatchMaxMessageNum() {
        return 10;
    }

    protected int getMaxRetryTimes() {
        return 3;
    }

    protected boolean isPrintBodyLog() {
        return true;
    }

    protected boolean preFilter(MessageView msgView) {
        return true;
    }

    protected boolean idempotentCheck(MessageView msgView) {
        return false;
    }

    protected void markIdempotent(MessageView msgView) {
    }

    protected void handleDeadLetter(MessageView deadMsg) {
    }

    protected void onConsumeSuccess(MessageView msgView) {}

    protected void onConsumeFail(MessageView msgView, Exception ex) {}

    /**
     * 优雅关闭
     */
    public void destroy() {
        running = false;
        if (pullExecutor != null) {
            pullExecutor.shutdown();
        }
        if (simpleConsumer != null) {
            try {
                simpleConsumer.close();
            } catch (IOException e) {
                log.error("SimpleConsumer close error", e);
            }
            log.info("[RocketMQ SimpleConsumer Destroy] group={}", getConsumerGroup());
        }
    }


    /**
     * 主动跳过消息，ack，不重试
     */
    public static class SkipMessageException extends RuntimeException {
        public SkipMessageException(String message) {
            super(message);
        }
    }
}