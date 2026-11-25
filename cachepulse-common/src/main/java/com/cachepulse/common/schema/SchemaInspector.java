package com.cachepulse.common.schema;

import com.cachepulse.common.event.EventIdentity;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 从业务库读取表结构元数据。快照 ID 的计算方式与拆分前一致（同一个 ObjectMapper 配置序列化后取 SHA-256），
 * 迁移过来的表结构快照才能对上。
 */
public final class SchemaInspector {
    private SchemaInspector() { }

    public static TableSchema inspect(String sourceId, String database, ObjectMapper json, Connection source, String table) throws SQLException {
        DatabaseMetaData metadata = source.getMetaData();
        List<TableSchema.ColumnDefinition> columns = columns(metadata, source.getCatalog(), table);
        List<String> keys = new ArrayList<>();
        try (ResultSet rs = metadata.getPrimaryKeys(source.getCatalog(), null, table)) {
            while (rs.next()) keys.add(rs.getString("COLUMN_NAME"));
        }
        if (columns.isEmpty() || keys.size() != 1) throw new IllegalArgumentException("Registered table must exist and have one primary key: " + table);
        TableSchema.ColumnDefinition key = columns.stream().filter(c -> c.name().equals(keys.get(0))).findFirst().orElseThrow();
        if (!List.of(Types.BIGINT, Types.INTEGER, Types.SMALLINT, Types.TINYINT, Types.VARCHAR, Types.CHAR).contains(key.jdbcType())
                || key.size() > 255) throw new IllegalArgumentException("Unsupported primary key type/size for table " + table);
        List<String> restrictions = new ArrayList<>();
        Set<String> foreignKeyColumns = new HashSet<>();
        Set<String> autoUpdatedColumns = new HashSet<>();
        try (ResultSet rs = metadata.getImportedKeys(source.getCatalog(), null, table)) {
            while (rs.next()) foreignKeyColumns.add(rs.getString("FKCOLUMN_NAME"));
        }
        try (ResultSet rs = metadata.getExportedKeys(source.getCatalog(), null, table)) {
            while (rs.next()) foreignKeyColumns.add(rs.getString("PKCOLUMN_NAME"));
        }
        if ("MySQL".equals(metadata.getDatabaseProductName())) {
            String currentUser;
            try (var statement = source.createStatement(); var rs = statement.executeQuery("SELECT CURRENT_USER()")) {
                rs.next(); currentUser = rs.getString(1);
            }
            int at = currentUser.lastIndexOf('@');
            String grantee = "'" + currentUser.substring(0, at).replace("'", "''") + "'@'" + currentUser.substring(at + 1).replace("'", "''") + "'";
            String privilegeSql = "SELECT (SELECT COUNT(*) FROM information_schema.USER_PRIVILEGES WHERE GRANTEE=? AND PRIVILEGE_TYPE='TRIGGER')"
                    + "+(SELECT COUNT(*) FROM information_schema.SCHEMA_PRIVILEGES WHERE GRANTEE=? AND TABLE_SCHEMA=? AND PRIVILEGE_TYPE='TRIGGER')"
                    + "+(SELECT COUNT(*) FROM information_schema.TABLE_PRIVILEGES WHERE GRANTEE=? AND TABLE_SCHEMA=? AND TABLE_NAME=? AND PRIVILEGE_TYPE='TRIGGER')";
            try (var statement = source.prepareStatement(privilegeSql)) {
                statement.setString(1, grantee); statement.setString(2, grantee); statement.setString(3, source.getCatalog());
                statement.setString(4, grantee); statement.setString(5, source.getCatalog()); statement.setString(6, table);
                try (var rs = statement.executeQuery()) {
                    rs.next(); if (rs.getLong(1) == 0) restrictions.add("Direct TRIGGER privilege required to verify trigger visibility");
                }
            }
            Map<String, String[]> text = new LinkedHashMap<>();
            try (var stmt = source.prepareStatement("SELECT COLUMN_NAME,CHARACTER_SET_NAME,COLLATION_NAME,GENERATION_EXPRESSION,COLUMN_TYPE,EXTRA FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=?")) {
                stmt.setString(1, source.getCatalog()); stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        String name = rs.getString(1);
                        text.put(name, new String[]{rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5)});
                        if (rs.getString(6).toLowerCase(Locale.ROOT).contains("on update current_timestamp")) {
                            autoUpdatedColumns.add(name);
                        }
                    }
                }
            }
            columns = columns.stream().map(c -> {
                String[] info = text.get(c.name());
                // Connector/J 会把 DEFAULT_GENERATED 时间戳也标为生成列；实际生成列有表达式。
                boolean generated = info[2] != null && !info[2].isBlank();
                boolean tinyInt = (c.jdbcType() == Types.BIT || c.jdbcType() == Types.BOOLEAN) && info[3].startsWith("tinyint");
                String typeName = tinyInt || info[3].startsWith("enum(") || info[3].startsWith("set(") ? info[3] : c.typeName();
                return new TableSchema.ColumnDefinition(c.name(), c.ordinal(), tinyInt ? Types.TINYINT : c.jdbcType(),
                        typeName, c.size(), c.scale(),
                        c.nullable(), c.autoIncrement(), generated, info[0], info[1]);
            }).toList();
            try (var stmt = source.prepareStatement("SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA=? AND EVENT_OBJECT_TABLE=?")) {
                stmt.setString(1, source.getCatalog()); stmt.setString(2, table);
                try (ResultSet rs = stmt.executeQuery()) { rs.next(); if (rs.getLong(1) > 0) restrictions.add("Table has triggers"); }
            }
        }
        columns = columns.stream().map(c -> new TableSchema.ColumnDefinition(c.name(), c.ordinal(), c.jdbcType(),
                c.typeName(), c.size(), c.scale(), c.nullable(), c.autoIncrement(), c.generated(),
                c.characterSet(), c.collation(), foreignKeyColumns.contains(c.name()))).toList();
        String restriction = restrictions.isEmpty() ? null : String.join("; ", restrictions);
        String id = EventIdentity.hash(encode(json, List.of("schema-v2", sourceId, database,
                table, keys.get(0), columns, restrictions)));
        return new TableSchema(id, sourceId, database, table, keys.get(0), columns, restriction,
                Set.copyOf(autoUpdatedColumns));
    }

    private static List<TableSchema.ColumnDefinition> columns(DatabaseMetaData metadata, String catalog,
                                                              String table) throws SQLException {
        List<TableSchema.ColumnDefinition> result = new ArrayList<>();
        String escape = metadata.getSearchStringEscape();
        String pattern = table.replace(escape, escape + escape).replace("_", escape + "_");
        try (ResultSet rs = metadata.getColumns(catalog, null, pattern, null)) {
            while (rs.next()) {
                if (!table.equals(rs.getString("TABLE_NAME"))) { continue; }
                result.add(new TableSchema.ColumnDefinition(rs.getString("COLUMN_NAME"), rs.getInt("ORDINAL_POSITION"),
                        rs.getInt("DATA_TYPE"), rs.getString("TYPE_NAME"), rs.getInt("COLUMN_SIZE"), rs.getInt("DECIMAL_DIGITS"),
                        rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls,
                        "YES".equals(rs.getString("IS_AUTOINCREMENT")), "YES".equals(rs.getString("IS_GENERATEDCOLUMN"))));
            }
        }
        result.sort(java.util.Comparator.comparingInt(TableSchema.ColumnDefinition::ordinal));
        return result;
    }

    private static String encode(ObjectMapper json, Object value) {
        try {
            return json.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot encode schema", e);
        }
    }
}
