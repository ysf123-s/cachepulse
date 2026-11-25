# CachePulse

基于 Binlog 的缓存一致性与数据变更订阅平台。业务写库不用改代码，由 binlog 驱动缓存失效和下游通知；同步链路落后或丢消息时能发现并降级；改错的数据能生成回滚 SQL。

## 当前状态

微服务拆分已完成（设计文档第 2～5 步）：417 项测试通过（需要外部 Redis、MySQL、Docker 的 38 项自动跳过），已部署在 Instabox 上，公网入口 [cdc.yusf.duckdns.org](https://cdc.yusf.duckdns.org/) 指向网关。原单体已删除。拆分设计和实测结果见 `docs/design/microservice-split.md`（第 14.1 节）。

## 结构

1 个平台 + 1 个接入组件 + N 个接入方：

| 模块 | 端口 | 内容 |
| --- | --- | --- |
| `gateway` | 18090 | 唯一入口：路由、登录换令牌、JWT 校验、身份透传、路由熔断、公开接口限流、两个管理页面 |
| `cache-sync-service` | 18091 | 消费 binlog 删缓存、心跳与丢失检测、发布新鲜度信号和新增主键（第一阶段单实例） |
| `archive-service` | 18092 | 归档变更、表结构快照、生成回滚 SQL；内部接口 `GET /internal/events/{eventId}` |
| `delivery-service` | 18093 | 自己的消费组匹配订阅、生成投递任务、投递、重试；重放经 OpenFeign 取历史事件 |
| `demo-user-service` | 18094 | 演示"读缓存"的接入方：用户、部门 |
| `demo-inventory-service` | 18095 | 演示"接收推送"的接入方：缺货待办 |
| `cachepulse-spring-boot-starter` | — | 接入组件：查询回填、防旧值回填、查库限流、Redis 熔断、布隆过滤器、分组注册 |
| `cachepulse-framework` | — | 公共框架：统一错误响应、Feign 默认配置、内部令牌、traceId |
| `cachepulse-common`、`cachepulse-cache-core` | — | 事件模型、解析器、Canal 连接；缓存协议（`@AutoCache`、key 布局、注册表、新鲜度信号） |

## 技术栈

| 类别 | 选型 |
| --- | --- |
| 基础 | JDK 17、Spring Boot 3.5.16 |
| 微服务 | Spring Cloud 2025.0.3、Spring Cloud Alibaba 2025.0.0.0（Nacos 3.0）、Spring Cloud Gateway、OpenFeign |
| 服务治理 | Resilience4j（熔断、降级）、Micrometer Tracing（traceId 日志串联，不部署追踪服务） |
| 数据 | MySQL 8 及以上、MyBatis-Plus 3.5.18.1、JdbcTemplate、Redis 6.2 及以上（Lettuce） |
| 变更采集 | Canal 1.1.8 → Kafka（kafka-clients 3.9.x，跟随 Spring Boot） |
| 监控 | Actuator、Micrometer、Prometheus |

## 目录

| 路径 | 内容 |
| --- | --- |
| `config/nacos/` | 发布到 Nacos 的配置（命名空间 `cachepulse`）；密码不在这里 |
| `docker/mysql/init/` | 建库脚本：`05-cp-archive.sql`、`06-cp-delivery.sql`、`07-cp-grant.sql`（CachePulse），`01`～`04` 为原项目 |
| `docs/` | 演示表、升级 SQL；`docs/design/` 为设计文档 |
| `scripts/local/` | 本机一键启停 |
| `scripts/` | 延迟、负载测量脚本 |
| `Dockerfile` | 单个服务的镜像：`docker build --build-arg SERVICE=<服务名> .` |

## 构建与测试

```powershell
mvn -B -DskipTests package   # 首次运行需要联网下载依赖
mvn -o -B test               # 缺少 Redis、MySQL、Docker 的测试会自动跳过
cd gateway
node src/test/js/cache-dashboard.test.cjs
node src/test/js/platform-dashboard.test.cjs
```

## 本机运行

1. 准备中间件：经 SSH 隧道访问 Instabox（见 `AGENTS.md`），或用 `docker-compose.yml` 起本地环境。
2. 在 MySQL 里执行 `docker/mysql/init/05-cp-archive.sql`、`06-cp-delivery.sql`、`07-cp-grant.sql`（只新建 `cp_archive`、`cp_delivery` 两个库，不改动业务库）。
3. 复制 `cachepulse-local.env.example` 为 `cachepulse-local.env` 并填写（已被 `.gitignore` 忽略）。
4. 启动：`pwsh scripts/local/start.ps1 -Build`；停止：`pwsh scripts/local/stop.ps1`。设置了 `NACOS_HOME` 时脚本会先启动 Nacos 3.0.3（端口 18848，控制台 18850），然后发布 `config/nacos/` 下的配置。
5. 打开 `http://127.0.0.1:18090/`，用管理员账号登录。

服务不读取根目录的 `application-local.yml`（那是原项目的配置，里面的消费组和平台库属于原项目）。为防止误连，服务启动时拒绝原项目的消费组（`canal-cache-sync-mq`、`platform-change-archive`、`platform-change-archive-v2`）和平台库 `cdc_platform`。

### 网关登录

管理员密码只存 BCrypt 哈希。先构建，再运行下面的命令，按提示输入密码（至少 12 位），把输出填到 `GATEWAY_ADMIN_PASSWORD_HASH`：

```powershell
java "-Dloader.main=com.cachepulse.gateway.PasswordHashTool" -cp gateway/target/gateway-0.1.0-SNAPSHOT.jar org.springframework.boot.loader.launch.PropertiesLauncher
```

JWT 密钥、内部令牌、Nacos 密钥都用随机值：

```powershell
[Convert]::ToBase64String([Security.Cryptography.RandomNumberGenerator]::GetBytes(48))
```

接口调用：`POST /auth/token`（`{"username":"admin","password":"..."}`）换取 30 分钟有效的令牌，之后请求带 `Authorization: Bearer <令牌>`。用户、部门的查询公开，按客户端 IP 限流（超过返回 429）；其余 `/api/**` 都要令牌。
