package com.cachepulse.demo.user.diagnostics;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.Intercepts;
import org.apache.ibatis.plugin.Invocation;
import org.apache.ibatis.plugin.Signature;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.Statement;
import java.util.Locale;
import java.util.function.BooleanSupplier;

@Component
@ConditionalOnProperty(name = "cachepulse.diagnostics.department-write-timing", havingValue = "true")
@Intercepts({
        @Signature(type = Executor.class, method = "update", args = {MappedStatement.class, Object.class}),
        @Signature(type = Executor.class, method = "close", args = {boolean.class}),
        @Signature(type = StatementHandler.class, method = "update", args = {Statement.class})
})
public class DepartmentWriteTiming implements Interceptor {
    private final ThreadLocal<Timing> current = new ThreadLocal<>();
    private final TransactionTemplate transaction;

    public DepartmentWriteTiming(DataSource dataSource) {
        transaction = new TransactionTemplate(new JdbcTransactionManager(dataSource) {
            @Override
            protected void doBegin(Object tx, org.springframework.transaction.TransactionDefinition definition) {
                long start = System.nanoTime();
                super.doBegin(tx, definition);
                current.get().begin = System.nanoTime() - start;
            }

            @Override
            protected void doCommit(DefaultTransactionStatus status) {
                long start = System.nanoTime();
                super.doCommit(status);
                current.get().commit = System.nanoTime() - start;
            }
        });
    }

    public Result measure(boolean explicitCommit, BooleanSupplier update) {
        Timing timing = new Timing();
        long start = System.nanoTime();
        current.set(timing);
        try {
            boolean updated = explicitCommit ? transaction.execute(status -> update.getAsBoolean())
                    : update.getAsBoolean();
            long total = System.nanoTime() - start;
            String header = format("write", total) + ", " + format("jdbc", timing.jdbc);
            if (explicitCommit) {
                header += ", " + format("begin", timing.begin) + ", " + format("commit", timing.commit);
            } else {
                header += ", " + format("acquire", timing.acquire) + ", " + format("hold", timing.hold);
            }
            return new Result(updated, header);
        } finally {
            current.remove();
        }
    }

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        Timing timing = current.get();
        if (timing == null) {
            return invocation.proceed();
        }
        if (invocation.getTarget() instanceof Executor executor) {
            if ("close".equals(invocation.getMethod().getName())) {
                try {
                    return invocation.proceed();
                } finally {
                    if (timing.acquiredAt != 0) {
                        timing.hold = System.nanoTime() - timing.acquiredAt;
                    }
                }
            }
            // MyBatis reuses this connection when it prepares the statement.
            long start = System.nanoTime();
            executor.getTransaction().getConnection();
            timing.acquiredAt = System.nanoTime();
            timing.acquire = timing.acquiredAt - start;
            return invocation.proceed();
        }
        long start = System.nanoTime();
        try {
            return invocation.proceed();
        } finally {
            timing.jdbc = System.nanoTime() - start;
        }
    }

    private static String format(String stage, long nanos) {
        return String.format(Locale.ROOT, "%s;dur=%.3f", stage, nanos / 1_000_000.0);
    }

    public record Result(boolean updated, String header) {}

    private static class Timing {
        long acquiredAt;
        long acquire;
        long hold;
        long jdbc;
        long begin;
        long commit;
    }
}
