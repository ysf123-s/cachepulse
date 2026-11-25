package com.cachepulse.common.event;

import com.cachepulse.common.model.BinlogColumn;
import com.cachepulse.common.model.BinlogEvent;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 一行变更里实际变化的字段。归档服务用它展示字段差异，投递服务用它判断订阅的 changedFields 条件，
 * 两边必须用同一套比较规则（数值列按数值比较，1 和 1.0 视为相同）。
 */
public record FieldChange(String field, boolean beforePresent, Object before,
                          boolean afterPresent, Object after) {

    public static List<FieldChange> differences(BinlogEvent event) {
        Map<String, Object> before = event.getBeforeData() == null ? Map.of() : event.getBeforeData();
        Map<String, Object> after = event.getAfterData() == null ? Map.of() : event.getAfterData();
        Set<String> fields = new LinkedHashSet<>(before.keySet());
        fields.addAll(after.keySet());
        List<FieldChange> changes = new ArrayList<>();
        for (String field : fields) {
            boolean had = before.containsKey(field), has = after.containsKey(field);
            Object oldValue = before.get(field), newValue = after.get(field);
            if (had != has || !sameValue(field, oldValue, newValue, event)) {
                changes.add(new FieldChange(field, had, oldValue, has, newValue));
            }
        }
        return changes;
    }

    private static boolean sameValue(String field, Object before, Object after, BinlogEvent event) {
        if (before == null || after == null) { return Objects.equals(before, after); }
        List<BinlogColumn> columns = event.getAfterColumns() == null ? event.getBeforeColumns() : event.getAfterColumns();
        if (columns != null && columns.stream().anyMatch(column -> column.name().equals(field)
                && List.of(Types.INTEGER, Types.BIGINT, Types.SMALLINT, Types.TINYINT, Types.DECIMAL, Types.NUMERIC)
                .contains(column.jdbcType()))) {
            return new BigDecimal(before.toString()).compareTo(new BigDecimal(after.toString())) == 0;
        }
        return Objects.equals(before, after);
    }
}
