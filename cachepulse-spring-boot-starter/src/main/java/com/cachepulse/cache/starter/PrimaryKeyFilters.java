package com.cachepulse.cache.starter;

import com.cachepulse.cache.core.PrimaryKeySink;
import com.cachepulse.cache.model.AutoCacheConfig;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 为开启 {@code @AutoCache(bloomFilter = true)} 的分组维护主键布隆过滤器。
 * 启动后在后台从数据库按主键分页读出全部主键，读完才启用；CDC 消费到 INSERT/UPDATE 时加入主键，读库期间也照常加入。
 * 未启用、尚未建好或建失败时一律视为“可能存在”，查询走正常通道，只是少了这层分流。
 *
 * <p>按单实例消费全部分区设计：多实例部署时，本实例收不到其他实例分区里的新增，刚插入的主键在本实例判定为不存在，
 * 会走窄通道查库，结果仍然正确。删除的主键不会移出过滤器，只是多一些误判。</p>
 */
@Slf4j
@Component
public class PrimaryKeyFilters implements PrimaryKeySink {
    /** approximateCount 是过滤器里不同主键的近似数，超过 expectedInsertions 后误判率上升。 */
    public record FilterStatus(boolean ready, long approximateCount, long expectedInsertions, long bits, int hashes,
                               Long buildMillis, String error) {
    }

    private static final class Entry {
        private final PrimaryKeyBloomFilter filter;
        private volatile boolean ready;
        private volatile Long buildMillis;
        private volatile String error;

        private Entry(PrimaryKeyBloomFilter filter) {
            this.filter = filter;
        }
    }

    private final Map<String, Entry> filters = new TreeMap<>();
    private final CacheLoaderRegistry loaders;
    private final long expectedInsertions;
    private final int pageSize;
    private final long retryMs;
    private volatile Thread builder;

    @Autowired
    public PrimaryKeyFilters(CacheAnnotationProcessor processor, CacheLoaderRegistry loaders,
                             @Value("${cache.bloom.enabled:true}") boolean enabled,
                             @Value("${cache.bloom.expected-insertions:100000}") long expectedInsertions,
                             @Value("${cache.bloom.false-positive-rate:0.01}") double falsePositiveRate) {
        this(processor, loaders, enabled, expectedInsertions, falsePositiveRate, 5_000, 30_000);
    }

    PrimaryKeyFilters(CacheAnnotationProcessor processor, CacheLoaderRegistry loaders, boolean enabled,
                      long expectedInsertions, double falsePositiveRate, int pageSize, long retryMs) {
        this.loaders = loaders;
        this.expectedInsertions = expectedInsertions;
        this.pageSize = pageSize;
        this.retryMs = retryMs;
        if (enabled) {
            for (String table : processor.getAllTables()) {
                AutoCacheConfig config = processor.getConfig(table);
                if (config != null && config.isBloomFilter()) {
                    filters.put(table, new Entry(new PrimaryKeyBloomFilter(expectedInsertions, falsePositiveRate)));
                }
            }
        }
    }

    private PrimaryKeyFilters() {
        this.loaders = null;
        this.expectedInsertions = 0;
        this.pageSize = 1;
        this.retryMs = 1;
    }

    public static PrimaryKeyFilters none() {
        return new PrimaryKeyFilters();
    }

    /** false 只在过滤器已建好且确定不存在时返回。 */
    public boolean mightContain(String table, long id) {
        Entry entry = filters.get(table);
        return entry == null || !entry.ready || entry.filter.mightContain(id);
    }

    /** CDC 新增或修改行时调用；主键不是整数时忽略。 */
    @Override
    public void add(String table, Object id) {
        Entry entry = filters.get(table);
        if (entry == null || id == null) {
            return;
        }
        if (id instanceof Number number) {
            entry.filter.add(number.longValue());
            return;
        }
        try {
            entry.filter.add(Long.parseLong(id.toString()));
        } catch (NumberFormatException ignored) {
            // 已注册分组只支持 Long 主键，非数字主键不会出现在缓存 key 中。
        }
    }

    public Map<String, FilterStatus> status() {
        Map<String, FilterStatus> status = new TreeMap<>();
        filters.forEach((table, entry) -> status.put(table, new FilterStatus(entry.ready,
                entry.filter.approximateCount(), expectedInsertions, entry.filter.bitCount(), entry.filter.hashCount(),
                entry.buildMillis, entry.error)));
        return status;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void buildInBackground() {
        if (filters.isEmpty()) {
            return;
        }
        Thread thread = new Thread(this::buildAll, "pk-bloom-builder");
        thread.setDaemon(true);
        builder = thread;
        thread.start();
    }

    @PreDestroy
    public void stop() {
        Thread thread = builder;
        if (thread != null) {
            thread.interrupt();
        }
    }

    void buildAll() {
        filters.forEach(this::build);
    }

    /** 数据库暂时不可用时每 retryMs 重试，直到建好；加载器不支持扫描主键时放弃，该表不分流。 */
    private void build(String table, Entry entry) {
        while (!Thread.currentThread().isInterrupted()) {
            long started = System.nanoTime();
            try {
                long rows = scan(loaders.get(table), entry.filter);
                entry.buildMillis = (System.nanoTime() - started) / 1_000_000;
                entry.error = null;
                entry.ready = true;
                log.info("Primary key bloom filter for {} ready: {} ids read in {} ms", table, rows, entry.buildMillis);
                return;
            } catch (UnsupportedOperationException e) {
                entry.error = e.getMessage();
                log.warn("Primary key bloom filter for {} disabled: {}", table, e.getMessage());
                return;
            } catch (RuntimeException e) {
                entry.error = e.getClass().getSimpleName() + ": " + e.getMessage();
                log.warn("Building primary key bloom filter for {} failed, retrying in {} ms: {}",
                        table, retryMs, entry.error);
                try {
                    Thread.sleep(retryMs);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private long scan(CacheDataLoader<?> loader, PrimaryKeyBloomFilter filter) {
        long after = Long.MIN_VALUE;
        long rows = 0;
        List<Long> page;
        do {
            page = loader.loadIdsAfter(after, pageSize);
            for (Long id : page) {
                if (id <= after) {
                    throw new IllegalStateException("loadIdsAfter must return ascending ids greater than afterId");
                }
                filter.add(id);
                after = id;
            }
            rows += page.size();
        } while (page.size() >= pageSize && !Thread.currentThread().isInterrupted());
        return rows;
    }
}
