package com.cachepulse.archive;

import com.cachepulse.common.schema.SchemaInspector;

import com.cachepulse.common.event.EventIdentity;
import com.cachepulse.common.platform.PlatformProperties;
import com.cachepulse.common.schema.TableSchema;


import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;

public class TableRegistry {
    private final PlatformProperties properties;
    private final ArchiveStore store;
    private final Map<String, TableSchema> tables;

    public TableRegistry(PlatformProperties properties, ArchiveStore store, List<TableSchema> schemas) {
        this.properties = properties;
        this.store = store;
        this.tables = new java.util.concurrent.ConcurrentHashMap<>();
        schemas.forEach(schema -> tables.put(schema.table(), schema));
    }

    public static TableRegistry capture(PlatformProperties properties, ArchiveStore store, DataSource business,
                                        DataSource platform) throws SQLException {
        try (Connection source = business.getConnection(); Connection target = platform.getConnection()) {
            if (!properties.getDatabase().equals(source.getCatalog())) {
                throw new IllegalArgumentException("platform.database must match the business datasource catalog");
            }
            if (properties.getDatabase().equalsIgnoreCase(target.getCatalog())
                    || PlatformProperties.LEGACY_PLATFORM_DATABASE.equalsIgnoreCase(target.getCatalog())) {
                throw new IllegalArgumentException("Archive storage must be its own database, not the business or original platform database");
            }
            List<TableSchema> schemas = new ArrayList<>();
            for (String table : properties.getTables()) {
                TableSchema schema = inspect(properties, store, source, table);
                store.saveSchema(schema);
                schemas.add(schema);
            }
            return new TableRegistry(properties, store, schemas);
        }
    }

    public static TableSchema inspect(PlatformProperties properties, ArchiveStore store, Connection source, String table) throws SQLException {
        return SchemaInspector.inspect(properties.getSourceId(), properties.getDatabase(), store.json(), source, table);
    }

    public void refresh(TableSchema schema) { tables.put(schema.table(), schema); }


    public TableSchema get(String database, String table) {
        return properties.getDatabase().equals(database) ? tables.get(table) : null;
    }

    public List<TableView> views() {
        ZoneId binlogTimeZone = ZoneId.of(properties.getBinlogTimeZone());
        return properties.getTables().stream().map(tables::get).map(schema -> {
            ArchivedChange.Recovery recovery = store.recoveryState(schema, null, binlogTimeZone);
            return new TableView(schema, store.invalidationReason(schema.snapshotId()), recovery.available(), recovery.reason());
        }).toList();
    }

    public record TableView(TableSchema schema, String invalidatedReason, boolean recoveryAvailable, String reason) { }
}
