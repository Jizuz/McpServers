package com.jizuz.mcpserver.manager;

import lombok.extern.slf4j.Slf4j;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Slf4j
@Service
public class WebCrawlManager {

    /**
     * 抓取网页正文，剔除脚本、样式、导航栏等噪声
     */
    public String fetchWebContent(String url) throws IOException {
        Document doc = Jsoup.connect(url)
                .timeout(10000)
                .userAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
                .get();

        // 移除不需要的标签
        doc.select("script,style,noscript,nav,footer,aside").remove();

        // 提取正文
        Elements paragraphs = doc.select("p, h1, h2, h3, h4, h5, h6, li");
        StringBuilder sb = new StringBuilder();
        for (Element p : paragraphs) {
            String text = p.text().trim();
            if (!text.isEmpty()) {
                sb.append(text).append("\n");
            }
        }
        String content = sb.toString();
        log.info("网页 {} 抓取完成，文本长度：{}", url, content.length());
        return content;
    }
}