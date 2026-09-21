package com.jizuz.mcpserver.web;

import com.jizuz.mcpserver.manager.DocumentParseManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@Slf4j
@RestController
@RequestMapping("/rag/doc")
@RequiredArgsConstructor
public class DocumentUploadController {

    private final DocumentParseManager documentParseManager;

    @PostMapping("/upload")
    public String upload(@RequestParam("file") MultipartFile file) throws Exception {
        String fileName = file.getOriginalFilename();
        documentParseManager.parseAndSend(file, fileName);
        return "ok，文档开始处理";
    }

    @PostMapping("/crawl")
    public String crawlWeb(@RequestParam String url) throws Exception {
        documentParseManager.parseWebPageAndSend(url);
        return "ok，网页开始抓取并向量化";
    }

}
