package com.cachepulse.cache.starter;

import com.cachepulse.cache.core.CacheKeys;
import com.cachepulse.cache.core.GroupRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/** 接入组件自动配置：业务服务引用 starter 后即可通过 CacheQueryService 读缓存。 */
@AutoConfiguration(after = RedisAutoConfiguration.class)
@ComponentScan("com.cachepulse.cache.starter")
public class CachePulseCacheAutoConfiguration {

    @Bean
    public GroupRegistry cachePulseGroupRegistry(StringRedisTemplate redis, ObjectMapper json) {
        return new GroupRegistry(redis, json);
    }

    @Bean
    public RemoteCacheFreshness cachePulseFreshness(StringRedisTemplate redis, ObjectMapper json) {
        return new RemoteCacheFreshness(redis, json);
    }

    @Bean(destroyMethod = "close")
    public CacheGroupRegistrar cachePulseRegistrar(CacheAnnotationProcessor groups, GroupRegistry registry,
                                                   RemoteCacheFreshness freshness,
                                                   @Value("${spring.application.name}") String owner) {
        return new CacheGroupRegistrar(groups, registry, freshness, owner);
    }

    /** 同步服务广播的新增主键加入本地布隆过滤器；消息丢了只会让刚插入的行多走一次窄通道。 */
    @Bean
    public RedisMessageListenerContainer cachePulseBloomListener(RedisConnectionFactory factory,
                                                                 CacheAnnotationProcessor groups, PrimaryKeyFilters filters) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(factory);
        container.addMessageListener(new BloomBroadcastListener(groups, filters),
                new PatternTopic(CacheKeys.BLOOM_CHANNEL_PREFIX + "*"));
        return container;
    }
}
