package com.cachepulse.cache.starter;

import com.cachepulse.cache.core.CacheGroups;
import com.cachepulse.cache.core.CacheKeys;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;

import java.nio.charset.StandardCharsets;

/**
 * 接收同步服务广播的新增主键（频道 cachepulse:bloom:{库名}.{表名}），加入本地布隆过滤器。
 * 只收本服务所连的库里、本服务注册过的分组：别的库里同名的表不加（设计文档第 6.3、6.6 节）。
 * 消息丢了只会让刚插入的行多走一次窄通道。
 */
public class BloomBroadcastListener implements MessageListener {
    private final CacheGroups groups;
    private final PrimaryKeyFilters filters;

    public BloomBroadcastListener(CacheGroups groups, PrimaryKeyFilters filters) {
        this.groups = groups;
        this.filters = filters;
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String[] target = CacheKeys.parseBloomChannel(new String(message.getChannel(), StandardCharsets.UTF_8));
        if (target == null || groups.getConfig(target[0], target[1]) == null) {
            return;
        }
        try {
            filters.add(target[1], Long.parseLong(new String(message.getBody(), StandardCharsets.UTF_8)));
        } catch (NumberFormatException ignored) {
            // 主键不是整数的表不维护布隆过滤器
        }
    }
}
