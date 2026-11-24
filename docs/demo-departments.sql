-- 与 users 放在同一个演示数据库中；应用不会自动建表，不修改已有 users 表。
CREATE TABLE IF NOT EXISTS departments (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    code VARCHAR(32)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
