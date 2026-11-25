package com.cachepulse.demo.inventory;

import org.h2.jdbcx.JdbcDataSource;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.jdbc.datasource.init.ScriptUtils;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.UUID;

/** H2 的 MySQL 模式，建表脚本取自 examples/stockout/schema.sql；不代表验证了 MySQL 兼容性。 */
final class StockoutTestDatabase {
    private StockoutTestDatabase() { }

    static JdbcDataSource create(String prefix) throws Exception {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + prefix + "_" + UUID.randomUUID().toString().replace("-", "")
                + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        initialize(dataSource);
        return dataSource;
    }

    private static void initialize(DataSource target) throws Exception {
        String sql = Files.readString(Path.of("examples/stockout/schema.sql"), StandardCharsets.UTF_8);
        sql = sql.substring(sql.indexOf("CREATE TABLE"))
                .replace(" CHARACTER SET ascii COLLATE ascii_bin", "")
                .replace(" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", "");
        try (Connection connection = target.getConnection()) {
            ScriptUtils.executeSqlScript(connection, new ByteArrayResource(sql.getBytes(StandardCharsets.UTF_8)));
        }
    }
}
