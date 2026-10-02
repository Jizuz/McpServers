package com.jizuz.mcpserver.manager;

import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.splitter.DocumentSplitters;
import dev.langchain4j.data.segment.TextSegment;

import java.util.List;

/**
 * 确定性分块工具：权威源落库与消费端索引写入共用同一分块逻辑，
 * 保证kb_doc行chunk_id（=分块下标）与Qdrant point id（docId_下标）、BM25索引key三方对齐
 */
public final class ChunkSplitter {

    private ChunkSplitter() {
    }

    public static List<String> split(String content, int chunkSize, int chunkOverlap) {
        List<TextSegment> segments = DocumentSplitters.recursive(chunkSize, chunkOverlap)
                .split(Document.from(content));
        return segments.stream().map(TextSegment::text).toList();
    }
}
