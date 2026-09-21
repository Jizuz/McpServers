package com.jizuz.mcpserver.manager;

import com.jizuz.mcpserver.models.mq.DocChunkVectorMsg;
import com.jizuz.mcpserver.mq.producer.DocVectorProducer;
import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.DocumentParser;
import dev.langchain4j.data.document.parser.apache.pdfbox.ApachePdfBoxDocumentParser;
import dev.langchain4j.data.document.splitter.DocumentSplitters;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DocumentParseManager {

    private final EmbeddingModel embeddingModel;
    private final DocVectorProducer docVectorProducer;
    private final WebCrawlManager webCrawlManager;

    @Value("${embedding.chunk.size}")
    private int chunkSize;

    @Value("${embedding.chunk.overlap}")
    private int chunkOverlap;

    /**
     * 本地文件上传入口
     * @param file 文件
     * @param fileName 文件名
     * @throws Exception
     */
    public void parseAndSend(MultipartFile file, String fileName) throws Exception {
        String docId = UUID.randomUUID().toString();
        String docType = getDocType(fileName);
        Document document;

        try (InputStream inputStream = file.getInputStream()) {
            if ("pdf".equals(docType)) {
                DocumentParser parser = new ApachePdfBoxDocumentParser();
                document = parser.parse(inputStream);
            } else {
                String text = new String(inputStream.readAllBytes());
                document = Document.from(text);
            }
        }
        splitAndSendSegments(document, docId, fileName, docType, null);
    }

    /**
     * 网页URL抓取入口
     * @param url 网页链接
     * @throws Exception
     */
    public void parseWebPageAndSend(String url) throws Exception {
        String docId = UUID.randomUUID().toString();
        String rawText = webCrawlManager.fetchWebContent(url);
        Document document = Document.from(rawText);
        String fileName = url;
        String docType = "webpage";
        splitAndSendSegments(document, docId, fileName, docType, null);
    }

    // ========== 公共：分片、向量化、发送RocketMQ ==========
    private void splitAndSendSegments(Document document, String docId, String fileName, String docType, Integer pageNum) throws Exception {
        var splitter = DocumentSplitters.recursive(chunkSize, chunkOverlap);
        List<TextSegment> segments = splitter.split(document);
        log.info("文档 {} 分片总数：{}", fileName, segments.size());

        for (TextSegment seg : segments) {
            String chunkId = UUID.randomUUID().toString();
            Response<Embedding> embeddingResp = embeddingModel.embed(seg.text());
            float[] array = embeddingResp.content().vector();
            List<Float> vector = new ArrayList<>(array.length);
            for (float f : array) {
                vector.add(f);
            }

            DocChunkVectorMsg msg = DocChunkVectorMsg.builder()
                    .docId(docId)
                    .chunkId(chunkId)
                    .content(seg.text())
                    .vector(vector)
                    .fileName(fileName)
                    .pageNum(pageNum)
                    .docType(docType)
                    .build();

            docVectorProducer.sendVectorMsg(msg);
        }
        log.info("文档 {} 全部分片消息投递完成 docId={}", fileName, docId);
    }

    private String getDocType(String fileName) {
        String lower = fileName.toLowerCase();
        if (lower.endsWith(".pdf")) return "pdf";
        if (lower.endsWith(".md")) return "md";
        if (lower.endsWith(".txt")) return "txt";
        throw new IllegalArgumentException("不支持的文档类型：" + fileName);
    }
}