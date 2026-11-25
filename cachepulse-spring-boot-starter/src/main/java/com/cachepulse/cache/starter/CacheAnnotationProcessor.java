package com.cachepulse.cache.starter;

import com.cachepulse.cache.core.CacheGroupDescriptor;
import com.cachepulse.cache.core.CacheGroups;
import com.cachepulse.cache.core.CacheKeys;
import com.cachepulse.cache.annotation.AutoCache;
import com.cachepulse.cache.model.AutoCacheConfig;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @AutoCache 注解处理器
 */
@Slf4j
@Component
public class CacheAnnotationProcessor implements CacheGroups {
    /** 扫描 @AutoCache 实体的包。 */
    @Value("${cachepulse.cache.base-package:com.cachepulse}")
    private String basePackage;

    /** @AutoCache 没有指定 database 时使用的业务库名。 */
    @Value("${cachepulse.cache.database:demo_db}")
    private String defaultDatabase;

    /** 所有分组共用：写入缓存时在过期时间上随机增加 0–N%。 */
    @Value("${cache.ttl-jitter-percent:10}")
    private int ttlJitterPercent;

    /**
     * 表名 → 缓存配置的映射
     */
    private final Map<String, AutoCacheConfig> tableConfigMap = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        scanAndRegister();
    }

    /**
     * 扫描所有带 @AutoCache 注解的类并注册
     */
    private void scanAndRegister() {
        ClassPathScanningCandidateComponentProvider scanner =
                new ClassPathScanningCandidateComponentProvider(false);
        scanner.addIncludeFilter(new AnnotationTypeFilter(AutoCache.class));

        for (var beanDefinition : scanner.findCandidateComponents(basePackage)) {
            try {
                Class<?> beanType = Class.forName(beanDefinition.getBeanClassName());
                AutoCache annotation = beanType.getAnnotation(AutoCache.class);
                if (annotation != null) {
                    registerConfig(beanType, annotation);
                }
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("Cannot load cache entity " + beanDefinition.getBeanClassName(), e);
            }
        }

        log.info("CacheAnnotationProcessor initialized with {} configs from package {}",
                tableConfigMap.size(), basePackage);
    }

    private void registerConfig(Class<?> clazz, AutoCache annotation) {
        AutoCacheConfig config = new AutoCacheConfig();
        config.setTable(annotation.table());
        config.setDatabase(annotation.database().isBlank() ? defaultDatabase : annotation.database());
        config.setEntityClass(clazz);
        config.setKeyTemplate(annotation.keyTemplate());
        config.setStrategy(annotation.strategy());
        config.setExpireSeconds(annotation.expireSeconds());
        config.setMaxSizeBytes(annotation.maxSizeBytes());
        config.setEnableVersionControl(annotation.enableVersionControl());
        config.setIndexKeys(Arrays.stream(annotation.indexKeys()).map(AutoCacheConfig.IndexKey::parse).toList());
        config.setTtlJitterPercent(ttlJitterPercent);
        config.setBloomFilter(annotation.bloomFilter());

        extractIdFieldInfo(clazz, config);
        validateConfig(config);
        if (tableConfigMap.containsKey(config.getTable())) {
            throw new IllegalStateException("Duplicate cache table: " + config.getTable());
        }
        // 主键缓存与二级索引缓存共用整表清空和浏览，所有命名空间之间不能互为前缀。
        List<String> prefixes = config.allPrefixes();
        for (int i = 0; i < prefixes.size(); i++) {
            String prefix = prefixes.get(i);
            CacheKeys.requireNotReserved(prefix);
            for (String other : prefixes.subList(i + 1, prefixes.size())) {
                CacheKeys.requireDisjoint(prefix, other);
            }
            for (AutoCacheConfig existing : tableConfigMap.values()) {
                for (String other : existing.allPrefixes()) {
                    CacheKeys.requireDisjoint(prefix, other);
                }
            }
        }
        tableConfigMap.put(config.getTable(), config);
        log.info("Registered @AutoCache for table: {}, strategy: {}",
                annotation.table(), annotation.strategy());

    }

    private void validateConfig(AutoCacheConfig config) {
        String context = "Invalid @AutoCache on " + config.getEntityClass().getName() + ": ";
        if (!config.getTable().matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalStateException(context + "table must be a single table name");
        }
        if (!config.getKeyTemplate().matches("[A-Za-z0-9_-]+(?::[A-Za-z0-9_-]+)*:#id")) {
            throw new IllegalStateException(context + "keyTemplate must use a fixed namespace followed by :#id");
        }
        if (config.getExpireSeconds() <= 0 || config.getMaxSizeBytes() <= 0) {
            throw new IllegalStateException(context + "expireSeconds and maxSizeBytes must be positive");
        }
        if (config.getTtlJitterPercent() < 0 || config.getTtlJitterPercent() > 100) {
            throw new IllegalStateException(context + "cache.ttl-jitter-percent must be between 0 and 100");
        }
        if (config.getIdFieldType() != Long.class && config.getIdFieldType() != long.class) {
            throw new IllegalStateException(context + "only Long/long primary keys are supported");
        }
        for (AutoCacheConfig.IndexKey index : config.getIndexKeys()) {
            if (!index.template().matches("[A-Za-z0-9_-]+(?::[A-Za-z0-9_-]+)*:#[A-Za-z_][A-Za-z0-9_]*")) {
                throw new IllegalStateException(context + "indexKeys must use a fixed namespace followed by :#column");
            }
            if (!hasColumn(config.getEntityClass(), index.column())) {
                throw new IllegalStateException(context + "index column not found on entity: " + index.column());
            }
        }
    }


    /** binlog 使用数据库列名：@TableField 指定的列名，或字段名的下划线形式。 */
    private static boolean hasColumn(Class<?> clazz, String column) {
        for (Field field : clazz.getDeclaredFields()) {
            TableField tableField = field.getAnnotation(TableField.class);
            String name = tableField != null && !tableField.value().isBlank() ? tableField.value()
                    : field.getName().replaceAll("([A-Z])", "_$1").toLowerCase(Locale.ROOT);
            if (name.equals(column)) {
                return true;
            }
        }
        return false;
    }

    private void extractIdFieldInfo(Class<?> clazz, AutoCacheConfig config) {
        Field annotatedId = null;
        // 优先查找 MyBatis-Plus @TableId 字段。binlog 数据使用数据库列名，所以优先使用 value。
        for (Field field : clazz.getDeclaredFields()) {
            TableId tableId = field.getAnnotation(TableId.class);
            if (tableId != null) {
                if (annotatedId != null) {
                    throw new IllegalStateException("Multiple @TableId fields in " + clazz.getName());
                }
                annotatedId = field;
            }
        }
        if (annotatedId != null) {
            String idColumn = annotatedId.getAnnotation(TableId.class).value();
            config.setIdField(idColumn.isBlank() ? annotatedId.getName() : idColumn);
            config.setIdFieldType(annotatedId.getType());
            return;
        }

        // 回退：查找名为 id 的字段
        try {
            Field idField = clazz.getDeclaredField("id");
            config.setIdField("id");
            config.setIdFieldType(idField.getType());
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException("No @TableId or id field in " + clazz.getName(), e);
        }
    }

    /**
     * 根据表名获取缓存配置
     */
    @Override
    public AutoCacheConfig getConfig(String tableName) {
        return tableConfigMap.get(tableName);
    }

    /**
     * 判断某表是否需要缓存同步
     */
    @Override
    public boolean hasConfig(String tableName) {
        return tableConfigMap.containsKey(tableName);
    }

    /**
     * 获取所有已注册的表名
     */
    @Override
    public Set<String> getAllTables() {
        return Collections.unmodifiableSet(tableConfigMap.keySet());
    }

    /** 写入 Redis 注册表的分组定义。 */
    public List<CacheGroupDescriptor> descriptors(String owner) {
        return tableConfigMap.values().stream().map(config -> CacheGroupDescriptor.of(owner, config)).toList();
    }
}
