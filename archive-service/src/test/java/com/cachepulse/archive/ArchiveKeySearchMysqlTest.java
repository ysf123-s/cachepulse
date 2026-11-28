package com.cachepulse.archive;

import com.cachepulse.common.platform.PlatformProperties;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The key search (UNION of per-branch ORDER BY ... LIMIT queries) on real MySQL; H2 accepts more syntax. Requires Docker. */
@Testcontainers(disabledWithoutDocker = true)
class ArchiveKeySearchMysqlTest {
    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse(System.getProperty("test.mysql.image", "mysql:8.0")))
            .withUsername("root").withPassword("test-password").withEnv("MYSQL_ROOT_HOST", "%").withDatabaseName("demo_db");
    private final PlatformProperties properties = new PlatformProperties();
    private JdbcTemplate jdbc;
    private ArchiveStore store;
    private long sequence;

    @BeforeEach
    void setUp() throws Exception {
        String url = "jdbc:mysql://" + mysql.getHost() + ":" + mysql.getMappedPort(3306) + "/";
        DriverManagerDataSource root = new DriverManagerDataSource(
                url + "demo_db?useSSL=false&allowPublicKeyRetrieval=true", "root", "test-password");
        new JdbcTemplate(root).execute("DROP DATABASE IF EXISTS cp_archive");
        try (var connection = root.getConnection()) {
            ScriptUtils.executeSqlScript(connection, new FileSystemResource("docker/mysql/init/05-cp-archive.sql"));
        }
        jdbc = new JdbcTemplate(new DriverManagerDataSource(
                url + "cp_archive?useSSL=false&allowPublicKeyRetrieval=true", "root", "test-password"));
        store = new ArchiveStore(jdbc, new ObjectMapper());
        properties.setDatabase("demo_db");
        properties.setTables(List.of("inventory", "users"));
    }

    @Test
    void keySearchMatchesBothKeyColumnsOnceNewestFirstAndPages() {
        long insert = row("inventory", "INSERT", "7", null);
        long keptKey = row("inventory", "UPDATE", "7", "7");
        row("inventory", "UPDATE", "9", "9");
        long otherTable = row("users", "DELETE", "7", "7");
        row("orders", "INSERT", "7", null);
        long keyChanged = row("inventory", "UPDATE", "8", "7");

        assertThat(sequences(search(null, "7", null, null, 20))).containsExactly(keyChanged, otherTable, keptKey, insert);
        ArchivedChange.Page first = search(null, "7", null, null, 3);
        assertThat(sequences(first)).containsExactly(keyChanged, otherTable, keptKey);
        assertThat(sequences(search(Long.valueOf(first.nextCursor()), "7", null, null, 3))).containsExactly(insert);
        assertThat(sequences(search(null, "7", "users", "DELETE", 20))).containsExactly(otherTable);
        assertThat(search(null, "' OR 1=1 --", null, null, 20).items()).isEmpty();
    }

    private ArchivedChange.Page search(Long before, String key, String table, String operation, int limit) {
        return store.search(properties, new ArchivedChange.Filter(before, table, key, operation, null, null, limit));
    }

    private static List<Long> sequences(ArchivedChange.Page page) {
        return page.items().stream().map(summary -> Long.valueOf(summary.sequence())).toList();
    }

    private long row(String table, String operation, String key, String beforeKey) {
        long n = ++sequence;
        String eventId = String.format("%064d", n);
        jdbc.update("INSERT INTO change_event (event_id,source_id,database_name,table_name,operation,primary_key_name,"
                        + "primary_key_value,before_primary_key_value,event_time_ms,schema_snapshot_id,kafka_topic,kafka_partition,"
                        + "kafka_offset,entry_index,row_index,event_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                eventId, properties.getSourceId(), properties.getDatabase(), table, operation, "id", key, beforeKey,
                1_700_000_000_000L + n, "s".repeat(64), "example", 0, n, 0, 0, "{}");
        return jdbc.queryForObject("SELECT sequence_id FROM change_event WHERE event_id=?", Long.class, eventId);
    }
}
