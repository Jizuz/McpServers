package com.jizuz.mcpserver.mq.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jizuz.mcpserver.dao.KbDocDao;
import com.jizuz.mcpserver.manager.KbDocIndexManager;
import com.jizuz.mcpserver.models.mq.RagDocChangeEvent;
import com.jizuz.mcpserver.mq.producer.DocVectorProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 文档变更事件消费者（权威源驱动双索引同步）：
 * 1. topic沿用doc_chunk_vector_topic，tag=TAG_ADD/TAG_UPDATE/TAG_DELETE与旧链路vector_write隔离；
 * 2. 幂等：msg_id内存去重 + 消息md5与权威源二次比对（乱序/过期旧消息直接跳过）；
 * 3. 重试：消费失败走BaseConsumer重试3次，超限进死信日志告警（每日BM25全量重建兜底收敛）。
 */
@Slf4j
@Component
public class RagDocChangeConsumer extends BaseConsumer<RagDocChangeEvent> {

    /** msg_id幂等去重集合（容量超限清空，靠权威源md5校验兜住重复） */
    private static final Set<String> HANDLED_MSG_IDS = ConcurrentHashMap.newKeySet();
    private static final int MAX_IDEMPOTENT_SIZE = 100_000;

    private final KbDocIndexManager kbDocIndexManager;
    private final KbDocDao kbDocDao;

    @Value("${rocketmq.proxy-endpoint}")
    private String proxyEndpoint;

    public RagDocChangeConsumer(ObjectMapper objectMapper,
                                KbDocIndexManager kbDocIndexManager,
                                KbDocDao kbDocDao) {
        super(objectMapper);
        this.kbDocIndexManager = kbDocIndexManager;
        this.kbDocDao = kbDocDao;
    }

    @Override
    protected void consume(RagDocChangeEvent event, MessageView msgView) {
        String eventType = event.getEventType() == null ? "" : event.getEventType();
        switch (eventType) {
            case RagDocChangeEvent.EVENT_ADD -> handleAdd(event);
            case RagDocChangeEvent.EVENT_UPDATE -> handleUpdate(event);
            case RagDocChangeEvent.EVENT_DELETE -> handleDelete(event);
            default -> throw new IllegalArgumentException("未知事件类型：" + eventType);
        }
    }

    /**
     * ADD：权威源md5比对通过后同步双索引
     */
    private void handleAdd(RagDocChangeEvent event) {
        if (isStale(event)) {
            throw new SkipMessageException("过期ADD消息跳过 docId=" + event.getDocId());
        }
        kbDocIndexManager.addDoc(event.getDocId(), event.getDocGroup(), event.getTitle(), event.getContent());
    }

    /**
     * UPDATE：删旧写新（权威源md5比对防旧消息覆盖新内容）
     */
    private void handleUpdate(RagDocChangeEvent event) {
        if (isStale(event)) {
            throw new SkipMessageException("过期UPDATE消息跳过 docId=" + event.getDocId());
        }
        kbDocIndexManager.updateDoc(event.getDocId(), event.getDocGroup(), event.getTitle(), event.getContent());
    }

    /**
     * DELETE：双索引按docId移除（幂等，重复消费无副作用）
     */
    private void handleDelete(RagDocChangeEvent event) {
        kbDocIndexManager.removeDoc(event.getDocId());
    }

    /**
     * 消息md5与权威源当前md5比对：无有效记录（已软删/不存在）或md5不一致（乱序旧消息）均视为过期
     */
    private boolean isStale(RagDocChangeEvent event) {
        String dbMd5 = kbDocDao.findDocMd5(event.getDocId());
        if (dbMd5 == null) {
            log.warn("权威源无有效记录（已软删或不存在），消息跳过 docId={}", event.getDocId());
            return true;
        }
        if (!dbMd5.equals(event.getContentMd5())) {
            log.warn("消息md5与权威源不一致（乱序旧消息），跳过 docId={}", event.getDocId());
            return true;
        }
        return false;
    }

    @Override
    protected TypeReference<RagDocChangeEvent> getTypeReference() {
        return new TypeReference<RagDocChangeEvent>() {};
    }

    @Override
    protected String getTopic() {
        return DocVectorProducer.TOPIC;
    }

    @Override
    protected String getConsumerGroup() {
        return "rag-doc-change-group";
    }

    @Override
    protected String getProxyEndpoint() {
        return proxyEndpoint;
    }

    /**
     * tag过滤：仅订阅事件消息，与旧链路vector_write互不干扰
     */
    @Override
    protected String getTagExpression() {
        return "TAG_ADD || TAG_UPDATE || TAG_DELETE";
    }

    @Override
    protected int getBatchMaxMessageNum() {
        return 10;
    }

    @Override
    protected int getMaxRetryTimes() {
        return 3;
    }

    /**
     * msg_id幂等：已处理过返回true（true=跳过，见BaseConsumer.handleMessage语义）
     */
    @Override
    protected boolean idempotentCheck(MessageView msgView) {
        return HANDLED_MSG_IDS.contains(msgView.getMessageId().toString());
    }

    @Override
    protected void markIdempotent(MessageView msgView) {
        if (HANDLED_MSG_IDS.size() > MAX_IDEMPOTENT_SIZE) {
            HANDLED_MSG_IDS.clear();
        }
        HANDLED_MSG_IDS.add(msgView.getMessageId().toString());
    }

    @Override
    protected void handleDeadLetter(MessageView deadMsg) {
        log.error("[RagDocChange] 死信消息 msgId={}，索引未同步，等待每日BM25全量重建兜底/人工处理", deadMsg.getMessageId());
    }
}
