# CachePulse

基于 Binlog 的缓存一致性与数据变更订阅平台。业务写库不用改代码，由 binlog 驱动缓存失效和下游通知；同步链路落后或丢消息时能发现并降级；改错的数据能生成回滚 SQL。

## 当前状态

第 1 步：从原项目「数据库变更处理与缓存同步平台」原样迁入，升级到 Spring Boot 3.5.16，作为过渡模块 `cachepulse-monolith` 运行，功能与原项目一致。之后按「1 个平台 + 1 个接入组件 + N 个接入方」拆分为微服务，拆完后删除过渡模块。

## 技术栈

| 类别 | 选型 |
| --- | --- |
| 基础 | JDK 17、Spring Boot 3.5.16 |
| 微服务 | Spring Cloud 2025.0.3、Spring Cloud Alibaba 2025.0.0.0（Nacos 3.0）、Spring Cloud Gateway、OpenFeign |
| 数据 | MySQL 8、MyBatis-Plus 3.5.18.1、JdbcTemplate、Redis 7（Lettuce） |
| 变更采集 | Canal 1.1.8 → Kafka（kafka-clients 3.6.2） |
| 监控 | Actuator、Micrometer、Prometheus |

## 目录

| 路径 | 内容 |
| --- | --- |
| `cachepulse-monolith/` | 过渡模块：原单体应用 |
| `docker/`、`docker-compose.yml` | 本地 MySQL、Redis、Kafka、Canal 环境 |
| `docs/` | 演示表和升级 SQL |
| `examples/` | 可选的缺货待办示例表 |
| `scripts/` | 延迟、负载测量和验收脚本 |

## 构建与测试

```powershell
mvn -B test                 # 首次运行需要联网下载依赖
mvn -o -B test              # 之后可离线运行；缺少 Redis、MySQL、Docker 的测试会自动跳过
cd cachepulse-monolith
node src/test/js/cache-dashboard.test.cjs
node src/test/js/platform-dashboard.test.cjs
```

## 本地配置

连接信息不写进仓库。项目根目录的 `application-local.yml` 已被 `.gitignore` 排除，用来覆盖 `cachepulse-monolith/src/main/resources/application.yml` 里的本机默认值；也可以用环境变量 `MYSQL_HOST`、`REDIS_HOST`、`KAFKA_BOOTSTRAP` 等覆盖。
