package com.jizuz.mcpserver.tools;

import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springaicommunity.mcp.annotation.McpTool;
import org.springaicommunity.mcp.annotation.McpToolParam;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Slf4j
@Component
public class WeatherTool {

    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${weather.api-url}")
    private String weatherApiUrl;

    @McpTool(description = "根据城市名称查询实时天气，返回温度、天气状况、风力、湿度等信息")
    public String queryWeather(@McpToolParam(description = "城市中文名，例如：北京、上海、广州") String city) {
        log.info("WeatherTool queryWeather start, city: {}", city);
        try {
            String url = String.format(weatherApiUrl, city);
            String res = restTemplate.getForObject(url, String.class);
            JSONObject json = JSONObject.parseObject(res);

            JSONObject current = json.getJSONArray("current_condition").getJSONObject(0);
            String weatherDesc = current.getJSONArray("weatherDesc").getJSONObject(0).getString("value");
            String temp = current.getString("temp_C");
            String wind = current.getString("windspeedKmph");
            String humidity = current.getString("humidity");

            log.info("WeatherTool queryWeather finish, weather: {}, temp: {}, wind: {}, humidity: {}",
                    weatherDesc, temp, wind, humidity);

            return String.format("【%s天气】天气状况：%s，温度：%s℃，风速：%skm/h，湿度：%s%%",
                    city, weatherDesc, temp, wind, humidity);
        } catch (Exception e) {
            return "天气查询失败：网络异常或城市名称错误，请检查输入城市名";
        }
    }

}
