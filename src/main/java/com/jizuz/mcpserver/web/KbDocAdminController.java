package com.jizuz.mcpserver.web;

import com.alibaba.fastjson.JSON;
import com.jizuz.mcpserver.dao.KbDocDao;
import com.jizuz.mcpserver.manager.DocumentParseManager;
import com.jizuz.mcpserver.manager.KbDocService;
import com.jizuz.mcpserver.manager.LocalBm25Manager;
import com.jizuz.mcpserver.dao.Entity.KbDoc;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 知识库管理后台接口（static/admin/index.html为本控制器页面，知识库文档唯一入口）：
 * 文档分页列表（分组/状态筛选、标题搜索）、上传/网页抓取/粘贴新增、编辑、软删、恢复、手动重建BM25。
 */
@Slf4j
@RestController
@RequestMapping("/rag/admin")
@RequiredArgsConstructor
public class KbDocAdminController {

    private final KbDocService kbDocService;
    private final KbDocDao kbDocDao;
    private final LocalBm25Manager localBm25Manager;
    private final DocumentParseManager documentParseManager;

    /**
     * 文档分页列表（行聚合为文档）
     */
    @GetMapping("/doc/page")
    public Map<String, Object> page(@RequestParam(required = false) String docGroup,
                                    @RequestParam(required = false) Integer status,
                                    @RequestParam(required = false) String keyword,
                                    @RequestParam(defaultValue = "1") int page,
                                    @RequestParam(defaultValue = "10") int size) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("success", true);
        result.put("total", kbDocDao.countDocs(docGroup, status, keyword));
        result.put("list", kbDocDao.pageDocs(docGroup, status, keyword, page, Math.min(size, 100)));
        log.info("/doc/page result : {}", JSON.toJSONString(result));
        return result;
    }

    /**
     * 文档详情（分块拼全篇，编辑回显）
     */
    @GetMapping("/doc/detail")
    public Map<String, Object> detail(@RequestParam String docId) {
        List<KbDoc> chunks = kbDocDao.findValidChunks(docId);
        if (chunks.isEmpty()) {
            throw new IllegalArgumentException("文档不存在或已删除：" + docId);
        }
        KbDoc head = chunks.get(0);
        String content = chunks.stream().map(KbDoc::getContent).collect(Collectors.joining());
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("success", true);
        result.put("docId", docId);
        result.put("docGroup", head.getDocGroup());
        result.put("title", head.getTitle());
        result.put("chunks", chunks.size());
        result.put("content", content);
        return result;
    }

    /**
     * 上传文档文件（分组必选）
     */
    @PostMapping("/doc/upload")
    public Map<String, Object> upload(@RequestParam("file") MultipartFile file,
                                      @RequestParam String docGroup) throws Exception {
        String fileName = file.getOriginalFilename();
        String docId = documentParseManager.parseAndSend(file, fileName, docGroup);
        return success("docId", docId);
    }

    /**
     * 网页URL抓取入库（权威源链路：先落库、后发ADD事件）
     */
    @PostMapping("/doc/crawl")
    public Map<String, Object> crawl(@RequestParam String url,
                                     @RequestParam String docGroup) throws Exception {
        return success("docId", documentParseManager.parseWebPageAndSend(url, docGroup));
    }

    /**
     * 粘贴文本新增文档
     */
    @PostMapping("/doc/add")
    public Map<String, Object> add(@RequestParam String title,
                                   @RequestParam String docGroup,
                                   @RequestParam String content) {
        return success("docId", kbDocService.uploadDoc(title, docGroup, content));
    }

    /**
     * 编辑文档（服务端md5判定，有变化才发UPDATE）
     */
    @PostMapping("/doc/update")
    public Map<String, Object> update(@RequestParam String docId,
                                      @RequestParam String title,
                                      @RequestParam String docGroup,
                                      @RequestParam String content) {
        boolean updated = kbDocService.updateDoc(docId, title, docGroup, content);
        Map<String, Object> result = success(null, null);
        result.put("updated", updated);
        return result;
    }

    /**
     * 删除文档（软删除，可恢复）
     */
    @PostMapping("/doc/delete")
    public Map<String, Object> delete(@RequestParam String docId) {
        kbDocService.deleteDoc(docId);
        return success(null, null);
    }

    /**
     * 恢复软删文档（重发ADD重建双索引）
     */
    @PostMapping("/doc/restore")
    public Map<String, Object> restore(@RequestParam String docId) {
        kbDocService.restoreDoc(docId);
        return success(null, null);
    }

    /**
     * 手动触发BM25从权威源全量重建
     */
    @PostMapping("/doc/rebuild-bm25")
    public Map<String, Object> rebuildBm25() {
        return success("size", localBm25Manager.rebuildFromDb());
    }

    private Map<String, Object> success(String key, Object value) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("success", true);
        if (key != null) {
            result.put(key, value);
        }
        return result;
    }

    /**
     * 业务参数异常统一返回（页面友好提示）
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public Map<String, Object> handleIllegalArgument(IllegalArgumentException e) {
        log.warn("管理后台参数异常：{}", e.getMessage());
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("success", false);
        result.put("message", e.getMessage());
        return result;
    }
}
