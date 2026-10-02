package com.jizuz.mcpserver.web;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.multipart.MaxUploadSizeExceededException;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 全局异常兜底：multipart解析发生在进入Controller之前（checkMultipart阶段，mappedHandler为空），
 * 控制器内局部@ExceptionHandler捕获不到，必须用@RestControllerAdvice全局处理。
 * 上传超过大小限制时返回友好JSON，避免裸413空body。
 */
@Slf4j
@RestControllerAdvice
public class GlobalUploadExceptionHandler {

    /**
     * 上传/请求体超过大小限制统一返回（页面友好提示，与KbDocAdminController异常处理风格一致）
     */
    @ExceptionHandler(MaxUploadSizeExceededException.class)
    public Map<String, Object> handleMaxUploadSize(MaxUploadSizeExceededException e) {
        log.warn("上传超过大小限制：{}", e.getMessage());
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("success", false);
        result.put("message", "文件超过大小限制（单文件最大50MB、总请求60MB），请拆分后再上传");
        return result;
    }
}
