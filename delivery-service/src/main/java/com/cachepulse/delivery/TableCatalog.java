package com.cachepulse.delivery;

import com.cachepulse.common.platform.PlatformProperties;
import com.cachepulse.common.schema.SchemaInspector;
import com.cachepulse.common.schema.TableSchema;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 投递服务自己读取的表结构元数据：生成去重键要主键列，校验订阅要字段列表。
 * 不保存表结构快照、不处理 DDL（这些归归档服务），所以归档服务挂了投递照常进行（设计文档第 6.4 节）。
 */
public class TableCatalog {
    private final PlatformProperties properties;
    private final DataSource business;
    private final ObjectMapper json;
    private final Map<String, TableSchema> tables = new ConcurrentHashMap<>();

    public TableCatalog(PlatformProperties properties, DataSource business, ObjectMapper json) {
        this.properties = properties;
        this.business = business;
        this.json = json;
    }

    public static TableCatalog load(PlatformProperties properties, DataSource business, ObjectMapper json) throws SQLException {
        TableCatalog catalog = new TableCatalog(properties, business, json);
        catalog.reload();
        return catalog;
    }

    /** 启动时读一次；表结构变了（DDL）以后重启投递服务或调用它重读。 */
    public void reload() throws SQLException {
        try (Connection source = business.getConnection()) {
            if (!properties.getDatabase().equals(source.getCatalog())) {
                throw new IllegalArgumentException("platform.database must match the business datasource catalog");
            }
            for (String table : properties.getTables()) {
                tables.put(table, SchemaInspector.inspect(properties.getSourceId(), properties.getDatabase(), json, source, table));
            }
        }
    }

    public TableSchema get(String database, String table) {
        return properties.getDatabase().equals(database) ? tables.get(table) : null;
    }

    public List<TableSchema> all() {
        return properties.getTables().stream().map(tables::get).toList();
    }
}
