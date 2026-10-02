package com.jizuz.mcpserver.manager;

import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.DocumentParser;
import dev.langchain4j.data.document.parser.apache.pdfbox.ApachePdfBoxDocumentParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.InputStream;

/**
 * 文档解析（pdf/md/txt/网页 → 纯文本）：
 * 解析产物统一交给权威源KbDocService落库并发事件；分块/向量化移到消费端（分块单一来源ChunkSplitter）。
 * 旧链路embedding+vector_write直发逻辑不再由上传触发，DocVectorQdrantConsumer保留用于消费存量消息。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DocumentParseManager {

    private final KbDocService kbDocService;
    private final WebCrawlManager webCrawlManager;

    /**
     * 本地文件上传入口（权威源链路）：解析文本 → 落库 → ADD事件
     * @param docGroup 文档分组
     * @return 文档ID
     */
    public String parseAndSend(MultipartFile file, String fileName, String docGroup) throws Exception {
        String content = extractText(file, fileName);
        return kbDocService.uploadDoc(fileName, docGroup, content);
    }

    /**
     * 网页URL抓取入口（权威源链路）
     * @param docGroup 文档分组
     * @return 文档ID
     */
    public String parseWebPageAndSend(String url, String docGroup) throws Exception {
        String rawText = webCrawlManager.fetchWebContent(url);
        return kbDocService.uploadDoc(url, docGroup, rawText);
    }

    // ========== 解析 ==========

    /**
     * 提取本地文件纯文本（pdf/md/txt，utf-8）
     */
    public String extractText(MultipartFile file, String fileName) throws Exception {
        String docType = getDocType(fileName);
        try (InputStream inputStream = file.getInputStream()) {
            if ("pdf".equals(docType)) {
                DocumentParser parser = new ApachePdfBoxDocumentParser();
                Document document = parser.parse(inputStream);
                return document.text();
            }
            return new String(inputStream.readAllBytes());
        }
    }

    private String getDocType(String fileName) {
        String lower = fileName.toLowerCase();
        if (lower.endsWith(".pdf")) return "pdf";
        if (lower.endsWith(".md")) return "md";
        if (lower.endsWith(".txt")) return "txt";
        throw new IllegalArgumentException("不支持的文档类型（仅支持pdf/md/txt）：" + fileName);
    }
}