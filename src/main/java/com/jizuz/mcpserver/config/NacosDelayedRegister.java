package com.jizuz.mcpserver.config;

import com.alibaba.cloud.nacos.registry.NacosServiceRegistry;
import com.alibaba.cloud.nacos.registry.NacosRegistration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class NacosDelayedRegister {

    @Value("${server.port:8090}")
    private Integer serverPort;

    private final NacosServiceRegistry registry;
    private final NacosRegistration registration;

    public NacosDelayedRegister(NacosServiceRegistry registry,
                                NacosRegistration registration) {
        this.registry = registry;
        this.registration = registration;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void delayedRegister() throws InterruptedException {
        // 延迟2秒，彻底避开 Nacos Client STARTING 时序BUG
        Thread.sleep(2000);

        // 强制覆盖为yml固定端口，杜绝 port=0
        registration.setPort(serverPort);

        // 新版唯一合法单参数注册
        registry.register(registration);
    }
}