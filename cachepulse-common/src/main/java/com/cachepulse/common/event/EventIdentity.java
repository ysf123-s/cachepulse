package com.cachepulse.common.event;

import com.cachepulse.common.model.BinlogEvent;
import com.cachepulse.common.model.KafkaCanalRecord;


import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

public final class EventIdentity {
    private EventIdentity() { }

    public static String of(String sourceId, KafkaCanalRecord record, BinlogEvent event) {
        return hash(String.join("\u0000", "event-v1", sourceId, record.topic(),
                Integer.toString(record.partition()), Long.toString(record.offset()),
                Integer.toString(event.getEntryIndex()), Integer.toString(event.getRowIndex())));
    }

    /** Stable across Kafka republishing and Canal's per-primary-key row splitting. */
    public static String sourceRowKey(String sourceId, BinlogEvent event, String key, String beforeKey) {
        if (event.getBinlogFile() == null || event.getBinlogFile().isBlank() || event.getBinlogPosition() <= 0) {
            throw new IllegalArgumentException("Archiving requires a real binlog file and positive position");
        }
        return hash(String.join("\u0000", "source-row-v1", sourceId, event.getDatabaseName(), event.getTableName(),
                event.getBinlogFile(), Long.toString(event.getBinlogPosition()), event.getEventType().name(),
                key, beforeKey == null ? "" : beforeKey));
    }

    /** Ordinal among changes of the same primary key at this source position, within one Canal message. */
    public static String sourceEventKey(String sourceRowKey, int occurrence) {
        return hash(sourceRowKey + "\u0000" + occurrence);
    }

    public static String hash(String text) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(text.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JDK must provide SHA-256", e);
        }
    }
}
