package com.jizuz.mcpserver.mq.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jizuz.mcpserver.manager.QdrantRestDocManager;
import com.jizuz.mcpserver.models.mq.DocChunkVectorMsg;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
public class DocVectorQdrantConsumer extends BaseConsumer<DocChunkVectorMsg>{

    private final QdrantRestDocManager qdrantRestDocManager;

    // 批量配置
    private static final int BATCH_SIZE = 20;
    private static final long BATCH_TIMEOUT_MS = 1000;

    private final BlockingQueue<DocChunkVectorMsg> bufferQueue = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService flushExecutor = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean running = new AtomicBoolean(true);

    @Value("${rocketmq.proxy-endpoint}")
    private String proxyEndpoint;

    public DocVectorQdrantConsumer(ObjectMapper objectMapper, QdrantRestDocManager qdrantRestDocManager) {
        super(objectMapper);
        this.qdrantRestDocManager = qdrantRestDocManager;
    }

    @PostConstruct
    public void startFlushTask() {
        // 定时刷盘任务
        flushExecutor.scheduleAtFixedRate(this::flushBuffer, 0, BATCH_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        // 初始化集合（Qdrant 不可用时仅告警，不阻断整个应用启动；消费链路写入失败已有重试兜底）
        try {
            qdrantRestDocManager.initCollection();
        } catch (Exception e) {
            log.warn("Qdrant 集合初始化失败，Qdrant 可能未就绪或端口未映射：{}", e.getMessage());
        }
    }

    /**
     * 消费单条消息，放入内存队列
     */
    @Override
    protected void consume(DocChunkVectorMsg message, MessageView msgView) {
        bufferQueue.add(message);
        // 达到批量阈值立刻刷入
        if(bufferQueue.size() >= BATCH_SIZE){
            flushBuffer();
        }
    }

    /**
     * 批量刷入Qdrant
     */
    private synchronized void flushBuffer() {
        if (bufferQueue.isEmpty()){
            return;
        }
        List<DocChunkVectorMsg> batchList = new ArrayList<>();
        bufferQueue.drainTo(batchList);
        try {
            qdrantRestDocManager.batchUpsert(batchList);
        } catch (Exception e) {
            log.error("批量写入Qdrant失败，消息数量:{}", batchList.size(), e);
            // 失败：逐个重试，失败的消息抛出异常，RocketMQ重试
            for(DocChunkVectorMsg msg : batchList){
                try{
                    qdrantRestDocManager.upsertSingle(msg);
                }catch (Exception ex){
                    log.error("单条写入失败 docId={}", msg.getDocId(), ex);
                    throw new RuntimeException("Qdrant写入异常", ex);
                }
            }
        }
    }

    @PreDestroy
    public void stopFlushTask() {
        if(running.compareAndSet(true, false)){
            flushBuffer();
            flushExecutor.shutdown();
        }
    }

    @Override
    protected TypeReference<DocChunkVectorMsg> getTypeReference() {
        return new TypeReference<DocChunkVectorMsg>() {};
    }

    @Override
    protected String getTopic() {
        return "doc_chunk_vector_topic";
    }

    @Override
    protected String getConsumerGroup() {
        return "doc-qdrant-write-group";
    }

    @Override
    protected String getProxyEndpoint() {
        return proxyEndpoint;
    }

    @Override
    protected String getTagExpression() {
        return "vector_write";
    }

    @Override
    protected int getMaxRetryTimes() {
        return 3;
    }

}
