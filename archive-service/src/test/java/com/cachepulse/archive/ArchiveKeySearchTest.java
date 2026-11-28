package com.cachepulse.archive;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Searching the archive by primary key value: one UNION branch per (table, key column). */
class ArchiveKeySearchTest {
    private ArchiveTestFixture fixture;
    private long sequence;

    @BeforeEach
    void setUp() throws Exception {
        fixture = new ArchiveTestFixture();
        fixture.properties.setTables(List.of("inventory", "users"));
    }

    @Test
    void keyMatchesEitherKeyColumnOnceNewestFirstAcrossConfiguredTables() {
        long insert = row("inventory", "INSERT", "7", null);
        long keptKey = row("inventory", "UPDATE", "7", "7");
        row("inventory", "UPDATE", "9", "9");
        long otherTable = row("users", "INSERT", "7", null);
        row("orders", "INSERT", "7", null);
        long keyChanged = row("inventory", "UPDATE", "8", "7");

        assertThat(sequences(search(null, "7", null, null, 20))).containsExactly(keyChanged, otherTable, keptKey, insert);
        assertThat(sequences(search(null, "8", null, null, 20))).containsExactly(keyChanged);
    }

    @Test
    void cursorPagesThroughEveryMatchOnce() {
        List<Long> expected = new ArrayList<>();
        for (int index = 0; index < 7; index++) {
            String table = index % 2 == 0 ? "inventory" : "users";
            expected.add(0, row(table, "UPDATE", index % 3 == 0 ? "70" : "7", "7"));
            row(table, "UPDATE", "5", "5");
        }
        List<Long> seen = new ArrayList<>();
        Long before = null;
        do {
            ArchivedChange.Page page = search(before, "7", null, null, 2);
            assertThat(page.items()).hasSizeLessThanOrEqualTo(2);
            seen.addAll(sequences(page));
            before = page.nextCursor() == null ? null : Long.valueOf(page.nextCursor());
        } while (before != null);
        assertThat(seen).containsExactlyElementsOf(expected);
    }

    @Test
    void tableAndOperationFiltersStillApply() {
        long inventoryUpdate = row("inventory", "UPDATE", "7", "7");
        long usersInsert = row("users", "INSERT", "7", null);
        long usersDelete = row("users", "DELETE", "7", "7");

        assertThat(sequences(search(null, "7", "users", null, 20))).containsExactly(usersDelete, usersInsert);
        assertThat(sequences(search(null, "7", null, "UPDATE", 20))).containsExactly(inventoryUpdate);
        assertThat(search(null, "7", "orders", null, 20).items()).isEmpty();
        assertThat(search(null, "' OR 1=1 --", null, null, 20).items()).isEmpty();
    }

    private ArchivedChange.Page search(Long before, String key, String table, String operation, int limit) {
        return fixture.store.search(fixture.properties, new ArchivedChange.Filter(before, table, key, operation, null, null, limit));
    }

    private static List<Long> sequences(ArchivedChange.Page page) {
        return page.items().stream().map(summary -> Long.valueOf(summary.sequence())).toList();
    }

    /** Inserts one archived row directly and returns its sequence id. */
    private long row(String table, String operation, String key, String beforeKey) {
        long n = ++sequence;
        fixture.jdbc.update("INSERT INTO change_event (event_id,source_id,database_name,table_name,operation,primary_key_name,"
                        + "primary_key_value,before_primary_key_value,event_time_ms,schema_snapshot_id,kafka_topic,kafka_partition,"
                        + "kafka_offset,entry_index,row_index,event_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                String.format("%064d", n), fixture.properties.getSourceId(), fixture.properties.getDatabase(), table, operation,
                "id", key, beforeKey, 1_700_000_000_000L + n, "s".repeat(64), "example", 0, n, 0, 0, "{}");
        return fixture.jdbc.queryForObject("SELECT sequence_id FROM change_event WHERE event_id=?", Long.class,
                String.format("%064d", n));
    }
}
