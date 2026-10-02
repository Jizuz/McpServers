package com.jizuz.mcpserver.tools;

import com.jizuz.mcpserver.manager.LocalBm25Manager;
import com.jizuz.mcpserver.manager.QdrantRestDocManager;
import com.jizuz.mcpserver.models.enums.DocGroupEnum;
import dev.langchain4j.model.embedding.EmbeddingModel;
import lombok.extern.slf4j.Slf4j;
import org.springaicommunity.mcp.annotation.McpTool;
import org.springaicommunity.mcp.annotation.McpToolParam;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class RagTool {

    private final EmbeddingModel embeddingModel;
    private final QdrantRestDocManager qdrantRestDocManager;
    private final LocalBm25Manager localBm25Manager;

    /** 向量召回条数 */
    private final int vectorTopN;

    /** BM25关键词召回条数 */
    private final int bm25TopN;

    /** RRF平滑常数（标准值60） */
    private final int rrfK;

    public RagTool(EmbeddingModel embeddingModel,
                   QdrantRestDocManager qdrantRestDocManager,
                   LocalBm25Manager localBm25Manager,
                   @Value("${rag.recall.vector-top-n:10}") int vectorTopN,
                   @Value("${rag.recall.bm25-top-n:10}") int bm25TopN,
                   @Value("${rag.recall.rrf-k:60}") int rrfK) {
        this.embeddingModel = embeddingModel;
        this.qdrantRestDocManager = qdrantRestDocManager;
        this.localBm25Manager = localBm25Manager;
        this.vectorTopN = vectorTopN;
        this.bm25TopN = bm25TopN;
        this.rrfK = rrfK;
    }

    /**
     * MCP工具：健康科普知识库多路召回检索（向量 + 本地BM25关键词，RRF融合排序，支持六分组过滤），仅服务健康科普助手场景
     * readOnlyHint=true：只读查询，不会修改数据；
     * idempotentHint幂等，多次调用结果一致
     */
    @McpTool(
            name = "search_knowledge_base",
            description = "健康科普知识库检索工具（向量+BM25关键词双路召回，RRF融合排序，支持按健康分组过滤），仅用于健康科普助手场景。当用户咨询疾病、症状、用药、饮食养生、就医建议等健康科普问题时调用，从知识库检索权威科普片段作为回答依据；若用户明确限定某类健康分组（如心血管、呼吸、儿童健康等）可通过docGroup过滤。超出健康科普范围的问题不要调用本工具，也不要编造知识库中没有的医学内容。",
            annotations = @McpTool.McpAnnotations(
                    readOnlyHint = true,
                    destructiveHint = false,
                    idempotentHint = true
            )
    )
    public String searchKnowledgeBase(
            @McpToolParam(description = "用户健康科普问题（如症状、疾病、饮食调理等），用于向量+关键词双路检索知识库") String query,
            @McpToolParam(description = "返回TopK文档片段，默认3，最大5", required = false) Integer topK,
            @McpToolParam(description = "文档分组过滤（可选，不传则检索全部分组）：cardio_health=心血管、resp_health=呼吸、ped_health=儿童健康、endo_health=内分泌、women_health=女性健康、common_living=通用居家健康", required = false) String docGroup
    ) {
        if (query == null || query.isBlank()) {
            return "检索失败：查询内容不能为空";
        }
        if (topK == null || topK < 1) {
            topK = 3;
        }
        if (topK > 5) {
            topK = 5;
        }
        String group = docGroup == null || docGroup.isBlank() ? null : docGroup.trim();
        if (group != null && !DocGroupEnum.isValid(group)) {
            return "检索失败：非法文档分组 " + group + "，可选：cardio_health/resp_health/ped_health/endo_health/women_health/common_living，或不传以检索全部";
        }
        log.info("RagTool searchKnowledgeBase start, query: {}, topK: {}, docGroup: {}", query, topK, group);

        // ===== 多路召回：任一路失败自动降级为另一路 =====
        List<Map<String, Object>> vectorHits = List.of();
        List<LocalBm25Manager.Bm25Hit> bm25Hits = List.of();
        boolean vectorOk = true;
        boolean bm25Ok = true;

        try {
            // 路1：DashScope向量化 + Qdrant向量检索
            float[] vector = embeddingModel.embed(query).content().vector();
            List<Map<String, Object>> hits = qdrantRestDocManager.search(toFloatList(vector), vectorTopN, group);
            vectorHits = hits == null ? List.of() : hits;
        } catch (Exception e) {
            vectorOk = false;
            log.error("RagTool 向量召回失败, query: {}", query, e);
        }

        try {
            // 路2：本地BM25关键词检索
            List<LocalBm25Manager.Bm25Hit> hits = localBm25Manager.search(query, bm25TopN, group);
            bm25Hits = hits == null ? List.of() : hits;
        } catch (Exception e) {
            bm25Ok = false;
            log.error("RagTool BM25召回失败, query: {}", query, e);
        }

        if (!vectorOk && !bm25Ok) {
            return "知识库检索失败：向量召回与BM25召回均不可用，请稍后重试";
        }
        if (vectorHits.isEmpty() && bm25Hits.isEmpty()) {
            log.info("RagTool searchKnowledgeBase empty result, query: {}", query);
            return "知识库中未检索到相关内容，请尝试换个问法，或先通过 /rag/admin/doc/upload 上传相关文档";
        }

        // ===== RRF融合：score(d) = Σ 1/(k + rank_i(d))，双路均命中者排前 =====
        List<FusedHit> fused = rrfFuse(vectorHits, bm25Hits, topK);

        // ===== 拼接上下文返回给大模型 =====
        StringBuilder context = new StringBuilder();
        context.append("====知识库多路召回结果（向量+BM25，RRF融合 k=").append(rrfK)
                .append("，分组:").append(group == null ? "全部" : group).append("）====\n");
        if (!vectorOk) {
            context.append("（提示：向量召回暂不可用，本次仅BM25关键词召回）\n");
        }
        if (!bm25Ok) {
            context.append("（提示：BM25召回暂不可用，本次仅向量召回）\n");
        }
        for (FusedHit hit : fused) {
            context.append("【RRF:").append(String.format("%.4f", hit.rrfScore()))
                    .append(" 向量:").append(hit.vectorScore() == null ? "-" : String.format("%.4f", hit.vectorScore()))
                    .append(" BM25:").append(hit.bm25Score() == null ? "-" : String.format("%.4f", hit.bm25Score()))
                    .append(" 命中:").append(hit.hitFrom())
                    .append(" 分组:").append(hit.docGroup() == null || hit.docGroup().isEmpty() ? "-" : hit.docGroup())
                    .append(" 来源:").append(hit.fileName())
                    .append("】\n")
                    .append(hit.content()).append("\n\n");
        }
        log.info("RagTool searchKnowledgeBase finish, vectorHit: {}, bm25Hit: {}, fused: {}",
                vectorHits.size(), bm25Hits.size(), fused.size());
        return context.toString();
    }

    /**
     * RRF（Reciprocal Rank Fusion）融合两路召回结果，按融合得分降序取TopK
     * 公式：score(d) = Σ 1/(k + rank_i(d))，rank从1计
     */
    private List<FusedHit> rrfFuse(List<Map<String, Object>> vectorHits,
                                    List<LocalBm25Manager.Bm25Hit> bm25Hits, int topK) {
        Map<String, HitBuilder> byPointId = new LinkedHashMap<>();

        // 向量路
        for (int rank = 0; rank < vectorHits.size(); rank++) {
            Map<String, Object> point = vectorHits.get(rank);
            String pointId = String.valueOf(point.get("id"));
            HitBuilder b = byPointId.computeIfAbsent(pointId, k -> new HitBuilder());
            if (!b.metaFilled) {
                Map<String, Object> payload = getPayload(point);
                b.content = String.valueOf(payload.getOrDefault("content", ""));
                b.fileName = String.valueOf(payload.getOrDefault("fileName",
                        payload.getOrDefault("title", "未知来源")));
                b.docGroup = payload.get("docGroup") == null ? "" : String.valueOf(payload.get("docGroup"));
                b.metaFilled = true;
            }
            b.vectorScore = toDouble(point.get("score"));
            b.fromVector = true;
            b.rrfScore += 1.0 / (rrfK + rank + 1);
        }

        // BM25路
        for (int rank = 0; rank < bm25Hits.size(); rank++) {
            LocalBm25Manager.Bm25Hit hit = bm25Hits.get(rank);
            HitBuilder b = byPointId.computeIfAbsent(hit.pointId(), k -> new HitBuilder());
            if (!b.metaFilled) {
                b.content = hit.content();
                b.fileName = hit.fileName();
                b.docGroup = hit.docGroup() == null ? "" : hit.docGroup();
                b.metaFilled = true;
            }
            b.bm25Score = hit.score();
            b.fromBm25 = true;
            b.rrfScore += 1.0 / (rrfK + rank + 1);
        }

        return byPointId.entrySet().stream()
                .map(e -> e.getValue().build(e.getKey()))
                .sorted((a, c) -> Double.compare(c.rrfScore(), a.rrfScore()))
                .limit(topK)
                .toList();
    }

    private List<Float> toFloatList(float[] vector) {
        List<Float> vectorList = new ArrayList<>(vector.length);
        for (float v : vector) {
            vectorList.add(v);
        }
        return vectorList;
    }

    private Double toDouble(Object value) {
        return value instanceof Number num ? num.doubleValue() : null;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getPayload(Map<String, Object> point) {
        Object payload = point.get("payload");
        return payload instanceof Map ? (Map<String, Object>) payload : Map.of();
    }

    /**
     * 融合后条目
     */
    private record FusedHit(String pointId, double rrfScore, Double vectorScore, Double bm25Score,
                            String content, String fileName, String docGroup, String hitFrom) {}

    /**
     * 融合构造器（可变累积）
     */
    private static final class HitBuilder {
        double rrfScore;
        Double vectorScore;
        Double bm25Score;
        String content = "";
        String fileName = "";
        String docGroup = "";
        boolean metaFilled;
        boolean fromVector;
        boolean fromBm25;

        FusedHit build(String pointId) {
            String hitFrom = fromVector && fromBm25 ? "向量+关键词" : (fromVector ? "向量" : "关键词");
            return new FusedHit(pointId, rrfScore, vectorScore, bm25Score, content, fileName, docGroup, hitFrom);
        }
    }

}
