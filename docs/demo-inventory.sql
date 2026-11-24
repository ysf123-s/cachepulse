-- 贯穿示例：inventory 表初始化脚本
-- 用于在已有演示环境 demo_db 中补充 inventory 表，演练事件归档、条件订阅、逆向恢复与重放

USE demo_db;

CREATE TABLE IF NOT EXISTS inventory (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    sku VARCHAR(64) NOT NULL,
    available_qty INT NOT NULL DEFAULT 0,
    remark VARCHAR(255),
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 插入演示初始库存
INSERT INTO inventory (id, sku, available_qty, remark) VALUES
    (1, 'SKU-APPLE-13', 100, 'Warehouse A stock'),
    (2, 'SKU-DELL-XPS', 5, 'Warehouse B stock')
ON DUPLICATE KEY UPDATE sku = VALUES(sku);
