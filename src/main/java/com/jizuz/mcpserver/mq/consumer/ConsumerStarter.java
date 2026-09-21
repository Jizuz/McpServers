package com.jizuz.mcpserver.mq.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.ClientException;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class ConsumerStarter {

    private final List<BaseConsumer<?>> consumerList;

    @EventListener(ContextRefreshedEvent.class)
    public void startAllConsumers() {
        for (BaseConsumer<?> consumer : consumerList) {
            try {
                consumer.init();
            } catch (ClientException e) {
                log.error("RocketMQ SimpleConsumer init fail, topic={}", consumer.getTopic(), e);
            } catch (RuntimeException e) {
                // SimpleConsumer.build() 启动失败时抛出的是 IllegalStateException（如
                // "Stream is already completed, no further calls are allowed"），不能让
                // 事件广播阶段抛异常导致整个应用启动失败
                log.error("RocketMQ SimpleConsumer init fail(runtime), topic={}", consumer.getTopic(), e);
            }
        }
    }

    @EventListener(ContextClosedEvent.class)
    public void stopAllConsumers() {
        for (BaseConsumer<?> consumer : consumerList) {
            consumer.destroy();
        }
    }

}
