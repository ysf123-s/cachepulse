-- 给 demo 应用账号授予 CachePulse 两个平台库的权限。Docker Compose 的 MySQL 首次启动时自动执行。
-- 两个服务的库各自独立，生产环境可以分别建账号：归档服务只要 cp_archive，投递服务只要 cp_delivery。

GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX ON cp_archive.* TO 'demo'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX ON cp_delivery.* TO 'demo'@'%';
FLUSH PRIVILEGES;
