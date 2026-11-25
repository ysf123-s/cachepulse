# CachePulse 微服务拆分设计

- 更新日期：2026-11-24
- 状态：第 2～5 步完成。代码按第 14 节第 7 条先写完、再统一测试；实现时的调整、部署和实测结果见第 14.1 节。2026-11-25：原单体的测试已迁到各模块（435 项通过，38 项需要外部依赖自动跳过），过渡模块已删除，CI 加上了页面脚本测试。
- 范围：从过渡模块 `cachepulse-monolith` 拆成微服务（第 2～5 步）、部署与切换（第 7 步）。余量场景（第 6 步）另出设计。

## 1. 目标与原则

结构是 1 个平台 + 1 个接入组件 + N 个接入方：

- **平台**：缓存同步、归档、投递三个服务，加网关和 Nacos。
- **接入组件**：`cachepulse-spring-boot-starter`，业务服务引用它读缓存。
- **接入方**：演示用的用户服务、库存服务；第 6 步换成余量服务。

拆分前后不变的原则：

1. 写路径零侵入：业务写库不改代码，缓存失效和下游通知都由 binlog 驱动，不做双写。
2. 每一刀拆分都要有实际理由：失败域不同（外部接收端故障不能拖累缓存失效）、扩容维度不同（投递按积压量扩容，归档受限于数据库写入；缓存同步对延迟敏感，第一阶段单实例，见第 6.11 节）、资源争用（原单体里归档和投递的 GC 停顿影响缓存失效延迟）。
3. 恢复 SQL 只生成、不执行。
4. 只宣称实测过的能力。

第一阶段不做：多级缓存（Caffeine）、Sentinel、Seata、Zipkin 等追踪服务（只做 traceId 日志串联）、RocketMQ/RabbitMQ、Spring Boot 4。

## 2. 技术栈与版本

| 组件 | 版本 | 选择理由 | 验证情况（2026-11-24） |
| --- | --- | --- | --- |
| JDK | 17 | 本机与服务器镜像一致 | — |
| Spring Boot | 3.5.16 | 见下方"版本约束" | 360 项离线测试通过 |
| Spring Cloud | 2025.0.3 | 与 Boot 3.5 官方配套 | 冒烟工程通过 |
| Spring Cloud Alibaba | 2025.0.0.0 | 对应 Boot 3.5 / Cloud 2025.0 | 冒烟工程通过 |
| Nacos 服务端 | 3.0.3 | 与 SCA 自带客户端 3.0.3 完全一致；SCA 2025.0 要求服务端 3.x | 本机开启鉴权实测，见第 9.1 节 |
| Spring Cloud Gateway | 4.3.x（WebFlux） | 新 starter 名 `spring-cloud-starter-gateway-server-webflux`，配置前缀 `spring.cloud.gateway.server.webflux.*` | 经 Nacos 路由 + JWT 实测通过 |
| OpenFeign + LoadBalancer | 4.3.x | 只用于确有需要的同步调用 | 经 Nacos 实测通过 |
| 熔断、降级 | Resilience4j（Spring Cloud CircuitBreaker） | Spring 官方实现，Feign 和网关都内置支持，不需要控制台 | 实测通过，见第 9.3 节 |
| 链路追踪 | Micrometer Tracing（Brave） | 只串联日志，不部署追踪服务 | 实测网关、服务、Feign 被调方共用同一个 traceId |
| JWT 校验 | `spring-boot-starter-oauth2-resource-server`（HS256） | Boot 自带，不另引 jjwt | 实测通过 |
| MyBatis-Plus | 3.5.18.1 | 业务表沿用 | 离线测试通过 |
| Canal | 1.1.8（服务端与客户端） | 已验证的链路 | — |
| kafka-clients | 3.9.2（跟随 Boot） | 原来固定 3.6.2，有 CVE-2025-27817 等已公开漏洞；见下方"版本约束" | 离线测试通过；真实链路待验证 |
| protobuf-java | 3.25.9 | Canal 自带 3.6.1（CVE-2024-7254 等） | 离线测试通过；真实消息待验证 |
| 监控 | Actuator、Micrometer、Prometheus | 沿用 | — |

版本约束：

- **Spring Boot 停在 3.5。** 3.5 的免费社区支持已于 2026-06-30 结束（商业支持到 2032-06-30）。选它是受 SCA 约束：SCA 最新的 2025.1.0.0 对应 Boot 4.0，而 4.0 的社区支持 2026-12-31 也会到期；Boot 4.1、4.2 SCA 还不支持。换到 4.0 只多一个月支持期，还要把 Jackson 2 迁到 Jackson 3（包名改为 `tools.jackson`）。升级 Boot 4 列为后续工作，等 SCA 支持 4.1 以后再做。
- **kafka-clients 不升到 4.x。** Canal 1.1.8 按 kafka-clients 2.4 编译，官方连接器在 `getListWithoutAck`、`getFlatListWithoutAck` 里调用了 4.0 已删除的 `KafkaConsumer.poll(long)`。项目子类 `RetryableKafkaCanalConnector` 已经改用 `poll(Duration)` 重写了这两个方法，但 Canal 本身没有针对 4.x 测试过，所以跟随 Boot 管理的 3.9.x。服务器上的 Kafka 服务端是 3.9.0。
- **排除 ZooKeeper。** `canal.client` 和 `canal.common` 都带进 ZooKeeper 3.5.6，只给 Canal 的 ZooKeeper 集群连接器用；项目只用 Kafka 连接器，在根 pom 里排除。
- **不用 SCA 自带的 Nacos 负载均衡器**（`spring.cloud.loadbalancer.nacos.enabled`）：本机实测 30 秒内没能选出实例。

## 3. 环境现状（2026-11-24 在 Instabox 上核实）

以 Instabox 为主环境。以下和原 README、连接指南的写法不同的地方，以这里为准：

| 项目 | 实际情况 | 影响 |
| --- | --- | --- |
| MySQL | 26.7.0，`binlog_format=ROW`，`binlog_row_image=FULL` | README 改为"MySQL 8 及以上" |
| Redis | 6.2.24；DB 0 有 12 个 key（原应用），DB 15 有 1 个 key | Lua 脚本和网关限流要在 6.2 上验证；README 改为"Redis 6.2 及以上" |
| Kafka | 服务端 3.9.0；topic `example` 6 个分区、单副本；**消息保留 1 天** | 切换后的回滚窗口不超过 1 天 |
| Canal | 1.1.8；配置文件里的过滤规则是 `.*\\..*`，但容器环境变量 `canal.instance.filter.regex=demo_db\..*` 覆盖了它，**实际只捕获 `demo_db`**（同样被环境变量覆盖的 MySQL 地址可以佐证：配置文件写 `127.0.0.1:3306`，实际连接的是 MySQL 容器）；分区规则 `demo_db\\..*:$pk$` | 平台库的写入不会进入 Kafka。配置文件和实际生效的规则不一致，重建容器时必须带上环境变量；第 6 步加业务库时见第 6.7 节 |
| 已有消费组 | `canal-cache-sync-mq`、`platform-change-archive-v2` | CachePulse 的消费组不能重名 |
| 已占用端口（本机回环） | 22、3000、3306、5432、5901、6379、8080、9092、11111、18081、18082 | **Nacos 3 控制台默认用 8080，和研究助手后端冲突** |
| 已有 MySQL 库 | 有其他项目留下的 `nacos`、`seata` 等库 | Nacos 不用 MySQL 存储，用自带的嵌入式存储，不碰这些库 |
| 内存 | 共 7.8GiB，可用 4.9GiB。原应用 445MiB，Canal 517MiB，MySQL 546MiB，Kafka 450MiB | 见第 10.3 节 |
| 磁盘 | 系统盘 16G，剩 2.8G（Docker 镜像在系统盘）；`/data` 剩 11G | 不新建镜像，见第 10.1 节 |
| 部署方式 | 原应用用 `eclipse-temurin` JDK 17 镜像，只读挂载 `app.jar` 和 `application-local.yml`，在内部网络 `instabox-copy_appnet` 里运行 | 新服务沿用同一方式 |
| 运行限制 | 免费版单次运行最长 8 小时，空闲 30 分钟自动暂停 | 暂停期间所有服务冻结，恢复后继续消费；压测期间要保持活动 |

## 4. 模块划分

| 模块 | 类型 | 内容（来自现有代码） |
| --- | --- | --- |
| `cachepulse-framework` | 公共框架 | 所有服务共用的微服务基础设施（第 9 节）：统一异常和错误响应、Feign 默认配置（超时、熔断、降级、错误解码、请求头透传）、内部接口令牌校验、traceId 回写响应头 |
| `cachepulse-common` | 公共库 | `model` 下的事件模型（`BinlogEvent`、`BinlogColumn`、`BinlogVersion`、`KafkaCanalRecord`、`UnprocessableEventException`）；`parser` 下的两个解析器；`CanalConnectorWrapper`、`RetryableKafkaCanalConnector`；`EventIdentity`；`TableRegistry` 中读取表结构元数据的部分（见第 6.4 节） |
| `cachepulse-cache-core` | 公共库 | 缓存协议：`@AutoCache`、`CacheStrategy`、`AutoCacheConfig`、key 布局、`VersionControllerWithLua` 等所有 Lua 脚本；新增分组注册表、新鲜度信号、布隆广播的数据格式 |
| `cachepulse-spring-boot-starter` | 接入组件 | 查询侧：`CachedQueryService`、`CacheQueryService`、`CacheLoaderRegistry`、`CacheDataLoader`、`CacheLoadGuard`、`QueryProtection`、`RedisCircuitBreaker`、`PrimaryKeyFilters`、`PrimaryKeyBloomFilter`、`/cache/protection`；新增分组注册、读取新鲜度信号、订阅布隆广播 |
| `cache-sync-service` | 平台服务 | `CanalCacheSyncService`、`CdcHeartbeatMonitor`、`DeadLetterStore`、`CacheBrowseService`、`CacheNamespacePurger`、`diagnostics` 包；`/cache/groups`、`/cache/freshness`、`/sync/status`、诊断接口；新增发布新鲜度信号和布隆广播 |
| `archive-service` | 平台服务 | `ArchiveConsumer`、`ArchiveService` 的归档部分、`ArchiveStore` 的归档表部分、表结构快照、`HistoryController`、`recovery` 包；新增内部接口 `GET /internal/events/{eventId}` |
| `delivery-service` | 平台服务 | `SubscriptionMatcher`、`SubscriptionController`、`DeliveryExecutor`、`replay` 包、`DemoWebhookController`；新增投递消费者和源事件去重表；调用归档服务的 Feign 客户端 |
| `gateway` | 平台服务 | 路由、管理接口 JWT 鉴权、公开接口限流，托管 `cache-dashboard.html`、`platform-dashboard.html` |
| `demo-user-service` | 接入方 | `users`、`departments` 的实体、Mapper、控制器、写服务和加载器，通过接入组件读缓存 |
| `demo-inventory-service` | 接入方 | `inventory` 和缺货待办（`StockoutController`、`StockoutStore`），作为订阅接收方 |

其他：

- `CheckpointManager`、`CanalCheckpointEntity`、`CanalCheckpointMapper`、`model.Checkpoint` 只给 Canal 直连模式用，已确认没有引用，第 2 步删除。
- 包名统一改为 `com.cachepulse`。组件保留的 Redis key 前缀从 `canal-cache:` 改为 `cachepulse:`；缓存数据的 key 仍由 `@AutoCache` 的模板决定。
- `ArchiveStore`（582 行）按表的归属拆成归档和投递两部分。

### 4.1 为什么是这 6 个应用

旧项目是一个应用、两部分功能（缓存同步、数据变更平台），里面还混着演示业务代码。判断标准：每拆出一个应用，至少要满足一条——故障影响范围不同、扩容方式不同、资源特点不同、接入方式不同。

| 应用 | 单独存在的理由 | 如果合并会怎样 |
| --- | --- | --- |
| gateway | 统一入口：鉴权、身份透传、限流、路由熔断。旧项目的管理接口没有鉴权 | 每个服务各自做鉴权，管理接口容易漏 |
| cache-sync-service | 对延迟敏感（缓存失效 P99）。原单体里它和归档、投递共用一个 JVM，原项目做 GC 停顿排查时确认过互相影响 | 归档、投递的负载会拖慢缓存失效 |
| archive-service | 写库吞吐型，受数据库写入能力限制；负责保存历史、生成回滚 SQL | — |
| delivery-service | 要调用外部接收端，对方变慢或故障是常态；按积压量和接收端数量扩容 | 接收端变慢时，投递占用的线程和连接会和归档争抢同一个进程的资源 |
| demo-user-service | 演示"读缓存"的接入方式：引用接入组件，跨进程依赖注册表和新鲜度信号，这正是拆分后要验证的核心 | 和平台放在同一个进程，就验证不了跨进程方案 |
| demo-inventory-service | 演示"接收推送"的接入方式：作为订阅的接收端，验证投递、重试、重放和故障注入 | 可以并入用户服务；单独存在是为了让两种接入方式互不依赖。理由在 6 个里最弱，第 6 步由余量服务替换 |

## 5. 事件流与消费组

```
业务写库（demo_db） ── binlog ──> Canal ──> Kafka topic example（6 个分区）
                                              │
        ┌─────────────────────────────────────┼──────────────────────────────┐
        ▼                                     ▼                              ▼
 cache-sync-service                    archive-service                delivery-service
 消费组 cachepulse-cache-sync          消费组 cachepulse-archive       消费组 cachepulse-delivery
 删缓存、心跳、丢失检测                 归档、表结构快照、恢复 SQL      匹配订阅、生成任务、投递、重试
        │                                     │                              │
        ▼                                     ▼                              ▼
 Redis：缓存、注册表、                   MySQL cp_archive               MySQL cp_delivery ──HTTP──> 接收方
 新鲜度信号、布隆广播                          ▲                              │
        ▲                                     └──── OpenFeign（重放取事件）──┘
        │
 接入方（引用 starter）：读缓存、读新鲜度信号、订阅布隆广播
```

三个消费组各自提交位点：任一服务变慢或停掉，不影响另外两个。

## 6. 拆开后要解决的跨进程问题

### 6.1 缓存分组配置在接入方，同步服务看不到

- 现状：`CacheAnnotationProcessor` 扫描本进程的 `@AutoCache`，同步服务和查询在同一个 JVM。
- 方案：接入方启动时把分组配置写进 Redis 注册表（`cachepulse:registry:groups`，字段为 `库名.表名`），和已注册的配置冲突（表名或 key 前缀重复）时启动失败。同步服务定期读取注册表。
- 新增的竞态处理：接入方刚注册、同步服务还没读到时，这张表的变更不会删缓存。所以注册后先不用缓存，等同步服务在新鲜度信号里确认已加载这个版本的注册表，再启用缓存。

### 6.2 新鲜度在同步服务里算，查询在接入方用

- 现状：查询直接调用同进程的 `CdcHeartbeatMonitor.cacheUsable()`。
- 方案：同步服务每 200ms 写一次 `cachepulse:freshness`（状态、各分区心跳、已加载的注册表版本），过期时间 3 秒；接入方读取后本地缓存 200ms。
- 代价：原单体里查询直接读同进程的状态，链路落后 3 秒就绕过缓存；拆分后多了写信号和本地缓存两段，最长约 3.4 秒。
- 信号不存在、已过期、或者状态不是 `FRESH`，接入方都绕过缓存直接查库。这样同步服务自己挂了（没有人在删缓存）也能降级。
- 用 Redis 的过期时间判断信号是否过期，不比较两台机器的时间戳，避免时钟偏差。

### 6.3 布隆过滤器的新增主键来自 CDC

- 现状：`CanalCacheSyncService` 第 485 行处理 INSERT 时直接写同进程的过滤器。
- 方案：同步服务通过 Redis Pub/Sub 广播新增主键（频道 `cachepulse:bloom:{库名.表名}`），接入方订阅后加入本地过滤器。
- 消息丢了不影响正确性：漏掉的主键只是走窄通道查库，现在的设计本来就不会直接拒绝请求。

### 6.4 归档和投递任务在同一个本地事务里

- 现状：`ArchiveService` 第 45～104 行，归档插入成功才生成投递任务，靠 `change_event.source_event_key` 唯一键去重。
- 方案：投递服务用独立消费组直接消费变更流，在 `cp_delivery` 里建一张源事件去重表（主键 `source_event_key`），和投递任务在同一个本地事务里写入。
- 只靠投递任务的唯一键 `(event_id, subscription_id, round_id)` 不够：`event_id` 由 Kafka 位点算出，Canal 重发同一条 binlog 时位点变了，会被当成新事件。`source_event_key` 由 binlog 文件和位置算出，不受重发影响。
- 投递服务生成去重键需要表的主键列，所以把 `TableRegistry` 中读取表结构元数据的部分移到 `cachepulse-common`，归档和投递各自读取；表结构快照和 DDL 失效仍然归归档服务。这样消费时不依赖归档服务，**归档服务挂了，投递照常进行**。
- 投递消息体就是变更事件本身（`store.encode(event)`），不含归档序号，投递服务可以独立生成，格式不变。
- 唯一的跨服务调用是重放：`ReplayService` 第 71 行要取历史事件，改为通过 OpenFeign 调用归档服务。

### 6.5 心跳行要和原项目分开（代码要改）

- 开发期间 CachePulse 和原项目会同时运行。两边都写心跳表 `demo_db.cdc_heartbeat`，必须使用不同的行：两个写入方写同一行时，只要时钟稍有偏差，后写的值可能比基线旧而被当成重复投递忽略，下一次比较就会误报丢消息。
- 现在 `CdcHeartbeatMonitor.beat()` 写死了 `WHERE id BETWEEN 1 AND ?`（第 210 行），要增加起始行号配置，CachePulse 用 1001～1024。
- 已按 Canal 的分区规则（库名哈希异或主键哈希，对 6 取模）算过：1001～1024 落到 6 个分区的行数是 4、3、4、4、4、5，每个分区至少 3 行；现有的 1～24 是 3、4、5、5、4、3。
- 原项目的心跳监控按表名识别心跳，不区分行号，所以它也会处理 1001～1024。每行只有一个写入方，不会误报；但如果 CachePulse 在本机运行、用本机时钟写心跳，本机时钟偏快时会让原项目误以为分区更新鲜。所以**开发期间不在本机启用心跳写入**，只在 Instabox 上运行时启用。

### 6.6 缓存同步只按表名匹配（代码要改）

- `CanalCacheSyncService.handleEvent` 只按表名找分组（第 458 行），心跳识别也只看表名（第 231 行）。现在 Canal 只捕获 `demo_db`，暂时不会出错；但第 6 步加入余量库以后，不同库里的同名表就会误删缓存。
- 方案：注册表和匹配都改为 `库名.表名`。

### 6.7 第 6 步新增业务库时要同时改 Canal 的两处配置

- 过滤规则：现在由容器环境变量决定，只改配置文件不会生效，要改环境变量并重建 Canal 容器。
- 分区规则：在配置文件里，新库的表不加规则就不会按主键散列，会落到哪个分区需要核实；如果集中在同一个分区，会推迟该分区的心跳，引发不必要的降级。
- 两处都会影响正在运行的原项目，要挑时间操作并观察原项目的心跳。平台库（`cdc_platform`、`cp_archive`、`cp_delivery`）始终不加进过滤规则，避免平台自己的写入回流到变更流里。

### 6.8 查询保护参数要能动态调整

- `QueryProtection` 现在通过构造函数的 `@Value` 读取参数，只能重启生效。
- 改为 `@ConfigurationProperties`，配置变化时重建信号量；正在使用的名额释放回旧的信号量。实测 Nacos 推送配置后，`@ConfigurationProperties` 约 3 秒更新。

### 6.9 刷新、混合策略需要业务实体类（第一阶段只支持删除策略）

- `@AutoCache` 有三种策略：删除（`DELETE_MODE`）、刷新（`REFRESH_MODE`）、混合（`HYBRID_MODE`，大 key 删除、其余刷新）。刷新时，同步服务要把 binlog 行数据转成业务实体类再序列化（`CanalCacheSyncService.toCacheJson`，第 785～792 行），保证和查询侧写入的缓存格式一致。
- 拆分后实体类在接入方，同步服务拿不到，无法保证格式一致。
- 第一阶段跨进程的分组只支持删除策略，注册刷新或混合策略时启动失败。现有的 `users`、`departments` 两个分组都是删除策略，行为不变。
- 以后要恢复刷新策略，需要接入方在注册表里登记字段映射和类型，由同步服务按登记内容生成缓存值，并补充格式一致性测试。

### 6.10 服务实例列表缓存

- Spring Cloud LoadBalancer 默认把实例列表缓存 35 秒，而 Nacos 客户端已经有一份由服务端推送更新的本地缓存。实测被调方下线后，调用方还往旧地址发了约 23 秒请求。
- 所有服务（包括网关）设置 `spring.cloud.loadbalancer.cache.enabled=false`，实测下线后 9ms 内不再往旧地址发请求。

### 6.11 缓存同步服务第一阶段只支持单实例

- 现状：心跳监控按"一个实例消费全部分区"设计（`expected-partitions` 等于分区数）。开第二个实例会出两个问题：
  1. 新鲜度信号互相覆盖：每个实例只消费一部分分区，却都写同一个 key，接入方读到的可能只是其中一个实例的情况。
  2. 心跳误报丢消息：每个实例都写同一批心跳行，按第 6.5 节的分析，两个写入方写同一行会误报。
- 第一阶段只部署一个实例，不宣称"缓存同步可以水平扩容"。
- 单实例可以接受的理由：
  1. 容量够：一个进程开 6 个消费线程，覆盖 topic 的全部 6 个分区（同一消费组最多也只能有 6 个消费者）。原单体在 32 路并发写入下测过缓存失效 P99 210ms。
  2. 挂了也不会读到旧数据：新鲜度信号 3 秒内过期，接入方自动绕过缓存直接查库，只是变慢，数据库由查库限流保护。容器设置自动重启，重启后从已提交的位点接着处理积压，心跳追上后才重新启用缓存。
  3. 这是"故障时安全降级"的单点，不是"故障时出错"的单点，面试时可以这样解释，并说明第 6.11 节的多实例方案。
- 以后做多实例：心跳只由一个实例写（用 Redis 锁选出写入者）；新鲜度按分区分别上报（`cachepulse:freshness:{分区}`，由当前消费该分区的实例写）；接入方要求全部分区都新鲜才使用缓存。

## 7. 数据归属

| 库 | 表 | 归属 |
| --- | --- | --- |
| `demo_db`（不变） | `users`、`departments` | demo-user-service |
| | `inventory`、缺货待办表 | demo-inventory-service |
| | `cdc_heartbeat` | cache-sync-service，只写 1001～1024 行 |
| `cp_archive`（新建） | `table_schema_snapshot`、`recovery_baseline`、`change_event`、`recovery_plan` | archive-service |
| `cp_delivery`（新建） | `event_subscription`、`delivery_task`、`delivery_attempt`、`replay_job`、新的源事件去重表 | delivery-service |

- 第 2～5 步不在 `demo_db` 做任何 DDL：DDL 会让原项目的恢复 SQL 失效。
- 新建库不影响原项目的恢复 SQL；Canal 只捕获 `demo_db`，所以平台库的写入不会进入变更流。

## 8. 开发期间与原项目共存

| 资源 | 原项目 | CachePulse |
| --- | --- | --- |
| 消费组 | `canal-cache-sync-mq`、`platform-change-archive-v2` | `cachepulse-cache-sync`、`cachepulse-archive`、`cachepulse-delivery` |
| Redis | DB 0，前缀 `canal-cache:` | DB 1，前缀 `cachepulse:` |
| 心跳行 | 1～24 | 1001～1024，只在 Instabox 上写入 |
| 平台库 | `cdc_platform` | `cp_archive`、`cp_delivery` |
| 应用端口 | 18081 | 见第 10.2 节 |

本机开发时经 SSH 隧道连接 Instabox 的中间件。Kafka 对外通告的地址是 `localhost:9092`，所以本地转发端口也必须是 9092。

## 9. 微服务基础框架

所有服务统一接入同一套基础设施：第 3 步先用一个最小示例服务把整套机制跑通，并写成自动化测试，再往里搬业务代码。公共代码放在 `cachepulse-framework`，公共配置放在 Nacos 的 `cachepulse-common.yaml`。标注"实测"的结论来自 2026-11-24 在本机做的冒烟验证（Nacos 3.0.3 开启鉴权，Spring Cloud 2025.0.3）。

### 9.1 注册与配置中心（Nacos）

- 单机模式，嵌入式存储，数据放在 `/data` 下。
- 必须开启鉴权：配置 `nacos.core.auth.enabled=true`、JWT 密钥 `nacos.core.auth.plugin.nacos.token.secret.key`（Base64，原文至少 32 字节）、服务端身份 `nacos.core.auth.server.identity.key/value`，首次启动后调用 `POST /nacos/v3/auth/user/admin` 初始化管理员密码。官方文档说明它是"防止误用的弱鉴权"，所以端口只在内部网络开放，不对公网暴露。
- 本机实测（3.0.3，鉴权开启）：

  | 项目 | 结果 |
  | --- | --- |
  | 启动耗时 | 约 32 秒 |
  | 内存 | `-Xmx512m` 时常驻约 708MB，630 个线程 |
  | 不带令牌发布配置、查询服务 | 均返回 403 |
  | 从 Nacos 读配置启动 | 通过 |
  | Feign 按服务名调用 | 410ms 内首次成功 |
  | 改配置后 `@ConfigurationProperties` 更新 | 约 3 秒 |
  | 被调方正常关闭 | Nacos 立即摘除实例 |

- 配置组织：命名空间 `cachepulse`；公共配置 `cachepulse-common.yaml`（连接地址、Redis DB 等），各服务 `<服务名>.yaml`（查询保护参数、订阅目标地址等）。**密码不放进 Nacos**，仍用环境变量或被忽略的本地配置文件。
- 配置导入不加 `optional:`：Nacos 不可用时服务启动失败，避免带着本地默认值上线。运行中 Nacos 短暂不可用，不影响已经加载的配置。
- 只有 `@ConfigurationProperties` 绑定的参数支持动态调整（实测约 3 秒生效）；连接池、端口这类启动参数改了要重启。

### 9.2 网关

网关去掉 `/api` 前缀后转发（`StripPrefix=1`），各服务的接口路径不变。

| 路径 | 转发到 | 鉴权 |
| --- | --- | --- |
| `/api/cache/protection` | demo-user-service（查询保护属于各接入方，每个接入方各有一份） | GET 公开，写操作 JWT |
| `/api/cache/**`、`/api/sync/**`、`/api/diagnostics/**` | cache-sync-service | GET 公开，写操作 JWT |
| `/api/platform/events/**`、`/api/platform/tables/**`、`/api/platform/recovery-plans/**`、`/api/platform/consumers/**` | archive-service | GET 公开，写操作 JWT |
| `/api/platform/subscriptions/**`、`/api/platform/deliveries/**`、`/api/platform/replay-jobs/**`、`/api/platform/targets/**`、`/api/platform/examples/**`、`/api/platform/demo/webhook/**` | delivery-service | GET 公开，写操作 JWT |
| `/api/users/**`、`/api/departments/**` 的 GET，以及按 ID 批量查询 `POST /api/users/batch`、`/api/departments/batch` | demo-user-service | 公开 |
| `/api/users/**`、`/api/departments/**` 的写操作 | demo-user-service | JWT |
| `/api/warehouse/**` | demo-inventory-service | GET 公开，写操作 JWT |
| `/internal/**` | 不转发 | 只在内部网络调用 |

所有路由都按客户端 IP 限流（每秒 50 次，突发 100 次，超过返回 429）。只读接口公开后，管理页面打开时不用登录；新建、修改、删除、重放、清缓存时才弹登录框（第 14.1 节第 13 条）。

`/platform/consumers/status` 现在一次返回缓存同步和归档两个消费者的状态。拆分后三个服务各自提供本服务消费者的状态，管理页面分别调用后合并展示；其中一个服务不可用时，其余两项照常显示。

- 签发令牌：网关提供 `POST /auth/token`，用单个管理员账号（密码存 BCrypt 哈希，HS256 密钥从环境变量读），令牌有效期 30 分钟。
- 身份透传：网关校验 JWT 后，先删掉客户端自带的 `X-User-Id`，再按 JWT 写入。实测客户端伪造 `X-User-Id: hacker`，下游收到的是 JWT 里的 `admin`。下游服务只认网关写入的身份头。
- 路由级熔断和降级：每条路由加 `CircuitBreaker` 过滤器，下游超时或熔断时返回 503（格式见第 9.5 节）。网关这一层的 TimeLimiter 默认只给 1 秒：实测 2 秒才响应的下游，1086ms 就返回了降级。所以要按路由显式设置超时，并和网关 HTTP 客户端的响应超时保持一致。
- traceId：网关生成 trace，通过 `traceparent` 传给下游（实测通过），同时写到响应头 `X-Trace-Id`，方便拿着 traceId 查日志。
- 限流：Gateway 自带的 `RequestRateLimiter` 加 `RedisRateLimiter`，用 Redis DB 1，需要在 Redis 6.2 上验证。按客户端 IP 计数，IP 取最前面那层反向代理写入的请求头（第 14.1 节第 12 条）。
- 两个管理页面由网关托管，页面里的接口地址改为 `/api/...`。

### 9.3 服务间调用（OpenFeign + Resilience4j）

- 调用点：平台内只有一个，投递服务重放时调用归档服务的 `GET /internal/events/{eventId}`。订阅字段校验改用本地的表结构元数据（第 6.4 节），不走 Feign。第 6 步的余量场景会增加业务服务之间的同步调用，同样遵守本节规则。
- **超时只有一处**：Feign 的连接超时 1 秒、读取超时 3 秒。关闭 Spring Cloud CircuitBreaker 默认的 TimeLimiter 和线程池（`spring.cloud.circuitbreaker.resilience4j.disable-time-limiter=true`、`disable-thread-pool=true`）。实测默认配置下，TimeLimiter 的 1 秒会覆盖 Feign 的读超时：读超时设为 3 秒，1.5 秒的调用在 1004ms 就失败了。关闭后正常返回，Feign 也改在请求线程里执行。
- 熔断：每个 Feign 方法一个熔断器，按调用次数统计失败率和慢调用率，超过阈值后熔断一段时间，再放少量请求试探。生产参数（窗口 20 次、最少 10 次、失败率 50%、熔断 10 秒、试探 3 次）放在 Nacos。实测（用小参数）：失败率超标后，后续请求直接降级、不再打到被调方；等待期满后试探成功即恢复。
- 降级：每个 Feign 客户端都要有 `FallbackFactory`。降级只抛出明确的"远程不可用"异常（映射为 503），不返回伪造的数据。重放遇到降级记为可重试失败，沿用现有 `recordReplayFailure(..., false)` 的处理。
- 重试：Feign 不自动重试，避免下游故障时请求被放大；需要重试的地方由业务显式处理（比如重放任务本身会重试）。
- 错误解码：`ErrorDecoder` 把下游返回的错误响应（第 9.5 节的格式）转成远程调用异常，保留下游的状态码、错误码和 traceId。
- 请求头透传：`RequestInterceptor` 透传 `X-User-Id`，`traceparent` 由 Micrometer 自动加上。实测被调方收到的用户头和 traceId 都正确。
- 内部接口：`/internal/**` 不经网关转发，只在内部网络调用；同时校验服务间共享的内部令牌（`X-Internal-Token`，从环境变量读取），防止内部网络里的其他容器误调用。

### 9.4 链路追踪与日志

- Micrometer Tracing（Brave），采样率 100%（流量小），不部署 Zipkin 等追踪服务。
- 日志默认带 `[traceId-spanId]`。实测入口服务和 Feign 被调方的日志 traceId 一致，可以按 traceId 把一次请求在各服务的日志串起来。注意：不额外配置时 Feign 不传 `traceparent`，必须引入 `feign-micrometer` 并开启 `spring.cloud.openfeign.micrometer.enabled`。
- 只覆盖 HTTP 调用链。binlog 经 Kafka 的异步链路没有 trace 上下文，仍用现有的 `CdcTrace` 分段诊断。README 不宣称"端到端链路追踪"。
- 各服务的日志写到 `/data` 下的挂载目录，按天滚动。

### 9.5 统一规范

- 错误响应：统一用 Spring 6 内置的 `ProblemDetail`（RFC 9457），字段为 `type`、`title`、`status`、`detail`，扩展 `code`（错误码）和 `traceId`。现在只有平台接口用它（`PlatformExceptionHandler`），缓存管理等接口还是 Spring 默认的错误格式，拆分时统一。管理页面只依赖 HTTP 状态码和原始响应体，不受影响。
- HTTP 状态码沿用现有约定（201、204、400、404、409、503），不改成"一律返回 200、在响应体里放业务码"。
- 异常分三层：客户端错误（4xx）、服务端错误（5xx）、远程调用错误（下游失败，502/503），由 `cachepulse-framework` 的全局异常处理统一转换。参考了 12306 的分层思路，代码自己实现。
- 参数校验：用 `jakarta.validation` 注解，校验失败统一返回 400 和字段错误列表。
- 幂等：消费侧沿用源事件键去重；会被重试的写接口要求带请求键（现有重放接口已经有 `requestKey`），重复请求返回第一次的结果。

### 9.6 健康检查与优雅上下线

- 健康检查：开启 Actuator 的存活和就绪探针（`/actuator/health/liveness`、`/actuator/health/readiness`）。就绪检查包含本服务必需的依赖（数据库、Redis、Kafka 消费者是否在运行）；存活检查不包含外部依赖，避免依赖抖动导致容器被反复重启。容器健康检查用就绪接口。
- 优雅下线：`server.shutdown=graceful`，等待处理中的请求完成，上限 20 秒。关闭时先从 Nacos 注销（实测立即摘除），调用方关掉了 LoadBalancer 缓存，9ms 内就不再发请求过来。Kafka 消费者停止拉取，处理完当前批次、提交位点后再退出（现有消费者的关闭逻辑第 4 步核实）。
- 启动顺序：Nacos 先启动；业务服务注册失败会重试；下游还没注册时，网关返回 503（实测），不会报错退出。

### 9.7 选型说明

- 熔断用 Resilience4j，不用 Sentinel：它是 Spring 官方的 Spring Cloud CircuitBreaker 实现，Feign 和网关都内置支持，不需要额外的控制台进程。
- 链路追踪只串联日志：Instabox 内存紧张，不部署追踪服务。
- 不用 Seata：平台的主张是由 binlog 驱动最终一致，不用分布式事务。

## 10. 部署（Instabox）

### 10.1 镜像

不新建镜像，所有服务沿用原应用的 `eclipse-temurin` JDK 17 镜像，各自只读挂载自己的 JAR 和配置，几乎不占系统盘。Nacos 用官方压缩包，在同一个镜像里运行，数据放在 `/data`。

### 10.2 端口（都只监听本机回环或内部网络）

| 服务 | 端口 |
| --- | --- |
| Nacos | 18848（HTTP）、19848（客户端 gRPC，固定为主端口 +1000）；控制台改到 18850，避开 8080 |
| gateway | 18090（唯一映射到宿主机的业务端口） |
| cache-sync-service、archive-service、delivery-service | 18091、18092、18093 |
| demo-user-service、demo-inventory-service | 18094、18095 |

服务之间在内部网络 `instabox-copy_appnet` 里通过 Nacos 注册的容器地址互相访问。公网入口 `cdc.yusf.duckdns.org` 目前指向原应用，切换时改指网关，具体转发链路切换前核实。

### 10.3 内存预算

| 项目 | 估算 |
| --- | --- |
| Nacos | 约 750MB（本机实测 708MB） |
| 6 个服务 | 每个堆 192～256MB，常驻 300～400MB，合计 1.8～2.4GB |
| 合计新增 | 2.6～3.2GB |
| 第 7 步验证多实例时 | 临时多一个演示用户服务实例，再加 300～400MB |

Instabox 现在可用 4.9GiB，共存期间原应用继续运行也放得下，剩余约 1.7～2.3GB。每个容器设置内存上限。ECS（2 核 2GB）放不下整套，第 7 步再定它上面跑什么。

### 10.4 各服务的实例数

"单实例"只针对缓存同步服务，其余服务都支持多开。第一阶段受内存限制每个服务先开 1 个，另外用演示用户服务验证一次多实例。

| 服务 | 第一阶段部署 | 能否多开 | 依据 |
| --- | --- | --- | --- |
| gateway | 1 | 能 | 无状态，JWT 自带身份，限流计数在 Redis |
| cache-sync-service | 1 | 暂不能 | 第 6.11 节 |
| archive-service | 1 | 能 | 同一消费组的实例按分区分摊；源事件键唯一约束去重 |
| delivery-service | 1 | 能 | 同上；投递任务按租约领取（`claimTasks`），多个实例不会重复领取同一任务 |
| demo-user-service | 第 7 步验证时开 2 个 | 能 | 无状态；布隆过滤器靠广播补新增主键；同 key 并发合并只在单个实例内 |
| demo-inventory-service | 1 | 能 | 无状态 |
| Nacos | 1（单机模式） | 集群要 3 个节点加外部 MySQL，第一阶段不做 | 内存 |

多实例验证（第 7 步）：演示用户服务开 2 个，经网关压测，确认请求分到两个实例；压测中停掉其中一个，确认请求很快全部转到另一个，并统计停掉期间有多少请求失败（关掉 LoadBalancer 缓存后，实测下线 9ms 内就不再往旧实例发请求）。

## 11. 切换方案（第 7 步，执行前另行确认）

前提：功能测试和真实环境测试通过，内存实测在预算内，原项目的心跳和丢失检测没有被干扰。

1. 停止原应用（两个消费者和投递执行器一起停），记录两个消费组在每个分区提交的位点。
2. 迁移旧数据：把 `cdc_platform` 的表复制到 `cp_archive`、`cp_delivery`（表结构相同，按第 7 节的归属拆到两个库）。
   - 用复制而不是移动，原库保持原样，回滚时原应用照常使用。
   - `platform.source-id` 保持和原项目一致，事件 ID 和源事件键才能接上，切换前的变更仍然可以查询、生成回滚 SQL。
   - 投递服务新增的源事件去重表从空开始：CachePulse 从原项目停下的位点接着消费，不会重复处理旧事件。未完成的投递任务随表迁移，租约到期后由投递服务重新领取。
   - 迁移后核对每张表的行数，并抽查历史查询、回滚 SQL、订阅列表。
3. 设置 CachePulse 三个消费组的位点（`kafka-consumer-groups.sh --reset-offsets`）：缓存同步取原 `canal-cache-sync-mq` 的位点，归档和投递取原 `platform-change-archive-v2` 的位点（原项目的投递任务由归档消费者生成），保证不漏不重。
4. 启动 CachePulse 的服务，确认心跳 `FRESH`、丢失检测开启、熔断闭合、布隆过滤器建好。
5. 公网入口改指网关。
6. 缓存从 Redis DB 0 换到 DB 1，相当于冷启动：缓存旁路模式下只会多一些查库。

回滚：保留原应用的 JAR、配置和 `cdc_platform` 库；原项目消费组的位点停在第 1 步。Kafka 只保留 1 天消息，所以回滚必须在 1 天内完成；切换后在 CachePulse 里新产生的归档和投递记录，回滚后不在原库里。

## 12. 实施步骤与验证

| 步骤 | 内容 | 验证 |
| --- | --- | --- |
| 2 | 包名改为 `com.cachepulse`；抽出 `common`、`cache-core`，过渡模块改为依赖它们 | 360 项测试照常通过，行为不变 |
| 3 | 微服务骨架：`cachepulse-framework`、Nacos、网关（路由、JWT、身份透传、路由熔断、令牌签发、限流）；用一个最小示例服务把第 9 节的机制全部跑通 | 把冒烟验证写成仓库里的自动化测试：超时只有一处、熔断开合、降级、伪造身份被覆盖、traceId 串联、Nacos 注册/读配置/动态调整/下线摘除、网关在下游未注册时返回 503；限流在 Redis 6.2 上验证；Nacos 相关测试在本机启动 Nacos 后运行 |
| 4 | 接入组件、注册表（含注册确认）、新鲜度信号、布隆广播、`库名.表名` 匹配、心跳起始行号；拆出 `cache-sync-service` 和 `demo-user-service`，接入骨架 | 先在真实链路上验证 kafka-clients 3.9.2 和 protobuf 3.25.9；单测加真实 Redis 测试（Redis 6.2）；在 Instabox 上改用户能删缓存，停掉同步服务 3 秒内绕过缓存 |
| 5 | 拆出归档和投递服务，拆分 `ArchiveStore`，加源事件去重表，重放改走 Feign；配置迁到 Nacos，查询保护参数动态调整，删除过渡模块 | 原有归档、投递、重放的流水线测试改造后通过；停掉归档服务后投递不受影响，重放走降级并记为可重试；重复投递同一条 binlog 只生成一次任务；经网关全链路走通 |
| 6 | 余量场景 | 另出设计 |
| 7 | 部署到 Instabox，隔离对照压测，迁移旧数据并切换（第 11 节） | 以原项目为基线，对比缓存失效 P99、降级切换时间；让投递接收端故障，看缓存失效 P99 是否受影响；迁移后核对行数，抽查历史查询、回滚 SQL 和订阅；演示用户服务开 2 个实例，验证负载分摊和停掉一个时的失败请求数（第 10.4 节） |

## 13. 待验证事项与风险

| 事项 | 计划 |
| --- | --- |
| kafka-clients 3.9.2、protobuf 3.25.9 处理真实 Canal 消息 | 第 4 步开始前，用独立消费组只读消费验证 |
| 升级后的 Redis 客户端（Lettuce 6.6）真实 Redis 测试 | 上次跑到一半被中断，可能留有少量随机前缀的临时 key，下次一并检查清理 |
| 网关 `RedisRateLimiter` 在 Redis 6.2 上的行为 | 第 3 步 |
| 服务进程被强杀时 Nacos 多久摘除实例 | 第 3 步；目前只测了正常关闭 |
| 第 6 步新增业务库时，Canal 的过滤规则（容器环境变量）和分区规则要一起改；不匹配分区规则的表落在哪个分区 | 第 6 步，要重建 Canal 容器，会影响原项目 |
| 本机时钟影响原项目的新鲜度判断 | 开发期间不在本机启用心跳写入 |
| Instabox 免费版自动暂停 | 压测期间保持活动；全天在线需要付费套餐 |
| Spring Boot 3.5 只剩商业支持 | 后续升级 Boot 4，等 SCA 支持 4.1 |

## 14. 决定记录

已确认（2026-11-24）：

1. **Canal 只捕获 `demo_db`**：核实后发现现状已经如此（容器环境变量覆盖了配置文件），不需要改服务器。第 6 步加业务库时按第 6.7 节处理。
2. **切换时迁移旧数据**：把 `cdc_platform` 复制到新库，步骤见第 11 节。
3. **管理接口用登录换令牌**：网关提供登录接口，管理页面加登录框，见第 9.2 节。
4. **保留 6 个应用**：拆分理由见第 4.1 节；曾考虑合并两个演示服务、或把归档和投递合回一个服务，结论是不合并。

5. **缓存同步服务第一阶段只开一个实例**（第 6.11 节）。
6. **工程能力只做现在必需的**：本机一键启停脚本、CI 加上页面脚本测试。Prometheus 采集、Flyway、OpenAPI 接口文档、Grafana 记为以后可选。
7. **实施节奏**：为了尽快用于面试，先把第 2～5 步的代码一次写完，最后统一测试；第 3 步的骨架和业务服务同时搭建，不再单独做示例服务。

### 14.1 实现时的调整（2026-11-24）

写代码时发现的问题和做法，和前面各节不一致的以这里为准：

1. **`SqlGenerator` 移到 `cachepulse-common`**（`com.cachepulse.common.schema`）。投递服务校验订阅的过滤值、匹配过滤条件要用它的取值格式化和比较规则，和归档服务必须一致。它是纯静态工具，不依赖归档的存储；生成回滚 SQL 仍然只在归档服务里调用。字段差异计算同理移为 `com.cachepulse.common.event.FieldChange`。
2. **按 ID 批量查询也公开**：`POST /users/batch`、`/departments/batch` 是只读查询，和单条查询一样公开、按 IP 限流（第 9.2 节表格已更新）。
3. **投递消费者状态**：`GET /platform/deliveries/consumer-status`，走投递服务的路由。管理页面分别调用 `/sync/status`、`/platform/consumers/status`、这个接口，某一项失败时其余照常显示。
4. **重放取事件**：归档服务返回 404（事件不在归档里）时 Feign 返回空（`dismiss404`），记为跳过，不算调用失败、不计入熔断；超时、5xx、熔断时整个重放任务记为可重试失败。取事件放在加行锁之前，不拿着行锁等远程调用。
5. **防止误用原项目的资源**：服务启动时拒绝原项目的消费组（`canal-cache-sync-mq`、`platform-change-archive`、`platform-change-archive-v2`）和平台库 `cdc_platform`；`CanalConnectorWrapper` 的默认消费组改为 `cachepulse-cache-sync`。根目录的 `application-local.yml` 是原项目的配置（指向 `cdc_platform` 和原项目的消费组），服务不启用 `local` profile、不读它；本机覆盖用 `cachepulse-local.yml`，密码用 `cachepulse-local.env`（都已忽略）。
6. **配置位置**：各服务 `application.yml` 写全本机默认值；Nacos 配置在 `config/nacos/`，用 `scripts/local/nacos-publish.ps1` 发布（Nacos 3 Admin API）；新建库的脚本是 `docker/mysql/init/05-cp-archive.sql`、`06-cp-delivery.sql`，去重表叫 `source_event`。
7. **投递服务的表结构元数据**：`TableCatalog` 启动时读一次；收到本库的 DDL 后重读，失败时沿用旧的字段列表并记日志。
8. **库存服务**：`StockoutController`、`StockoutStore` 去掉了 `stockout-demo` profile 限制，这是该服务的全部功能。
9. **令牌请求头**：管理页面把令牌放在 `X-CachePulse-Token`，网关也接受 `Authorization: Bearer`。原因是公网入口（华为云 Nginx → Instabox 端口 18082）会把 `Authorization` 换成访问 Instabox 端口的凭据，浏览器带的令牌到不了网关。网关转发前两个头都删掉。
10. **部署和公网入口（2026-11-24）**：在 Instabox 上按第 10 节部署，部署脚本在 `records/cachepulse-deploy-20261124/`，服务器目录 `/data/cachepulse`。原项目的 `cdc-platform-app`、`cdc-stockout-drill` 已停止（未删除，停止时的位点在服务器上的 `/data/cachepulse/records/old-offsets-*.txt`），旧数据没有迁移（第 11 节第 2 步未做）。公网 `cdc.yusf.duckdns.org` 已改指网关：Instabox 上 `cdc-huawei-web.service` 转发到 18090，华为云 Nginx 不再用本机存的旧版管理页面。
11. **Instabox 上的端到端验证（2026-11-24，`records/cachepulse-deploy-20261124/e2e.py`）**：心跳已开启（1001～1024 行，重启后约 21 秒 `FRESH`，丢失检测 `ON`）。用临时部门测试，测完已删除：

    | 项目 | 结果 |
    | --- | --- |
    | 改部门后缓存 key 被删 | 41ms、102ms（两次） |
    | 订阅投递到 demo-webhook | 0.6 秒内成功；投递任务和归档的 eventId 相同 |
    | 重放（经 Feign 取归档事件） | 立即完成，命中 1 条 |
    | 停掉缓存同步服务 | 新鲜度信号 2.3 秒后消失，之后的查询绕过缓存 |
    | 停掉归档服务 | 投递照常成功；重放记为可重试（下游 503），归档恢复后 14 秒自动完成，归档补齐了停机期间的变更 |

    第 13 节的"kafka-clients 3.9.2、protobuf 3.25.9 处理真实 Canal 消息"和"网关限流在 Redis 6.2 上"已验证。protobuf 3.25.9 读 Canal 的旧生成代码时会打印 `Vulnerable protobuf generated type` 警告，不影响处理。
12. **测试迁移时发现的问题（2026-11-25）**：
    - 已修：`cachepulse-framework` 自建的 `ObjectMapper` 没注册 ProblemDetail 的 mixin，真实服务的错误体把 `code`、`traceId` 套在 `properties` 里，Feign 错误解码读不到下游的错误码。已注册 `ProblemDetailJacksonMixin`（其他 JSON 不变，表结构快照 ID 不受影响），归档服务的启动测试检查了真实错误体。
    - 已修（部署后在服务器上发现）：控制器抛的 `ResponseStatusException` 被 Spring Boot 自带的 `ProblemDetailsExceptionHandler` 先处理，错误体没有 `code`、`traceId`。原因是框架的自动配置排在 `WebMvcAutoConfiguration` 之后，Spring Boot 没看到我们的处理器就注册了自己的。已把框架排到它前面，并给 Spring 自带异常补上 `code`（HTTP 状态名）和 `traceId`。
    - 已修（2026-11-25）：分组改按 `库名.表名` 存（`RegistryCacheGroups`），不同库的同名表各是一个分组；行事件、DDL 清缓存、新增主键广播都按 `库名.表名` 匹配。DDL 清缓存的库名取事件所在的库和 SQL 里出现的库名，拿不准时宁可多清。新增主键的广播频道改为 `cachepulse:bloom:{库名}.{表名}`，和第 6.3 节一致，接入组件只收本服务所连库里的分组。管理接口照旧可以只写表名，同一个表名在多个库都有分组时要写 `库名.表名`（否则返回 400）。
    - 已修（2026-11-25）：公网的查询限流原来按连接来源计数，而公网请求经华为云 Nginx → Instabox 端口转发 → 本机 TCP 转发进来，网关看到的来源都是 `172.19.0.1`，所有外网用户共用一个额度。改为按华为云 Nginx 写入的 `X-CachePulse-Client-IP`（`$remote_addr`，会覆盖客户端自带的值）计数，网关用 `cachepulse.gateway.client-ip-header` 配置这个头；没有这个头时仍按连接来源。网关只能经这层代理从公网访问，所以这个头可信。
    - 刷新、混合策略的代码（`doRefresh`、`doHybrid`、`toCacheJson`）在拆分后走不到（第 6.9 节），保留到以后恢复刷新策略时再用。
13. **只读接口公开（2026-11-25）**：从服务导航打开管理页面时，页面一加载就弹登录框，因为页面的查询接口也要令牌。改为 `/api/**` 的 GET 都公开，写操作（新建、修改、删除订阅，重放，清缓存，改用户和部门）仍要令牌，`/internal/**` 照旧拒绝。只读接口公开后，所有路由都按客户端 IP 限流。代价：谁都能看到归档的变更记录、订阅配置和缓存内容；这些都是 `demo_db` 的演示数据。`SecurityRulesTest` 检查了这几条规则。

## 15. 与原项目的对比

### 15.1 代码改动量

按现有 87 个源文件、8259 行统计：

| 类别 | 文件数 | 行数 | 说明 |
| --- | --- | --- | --- |
| 原样搬迁 | 68 | 4188（51%） | 只改包名和所在模块 |
| 需要修改 | 15 | 3861（47%） | 多数是局部修改，见下表 |
| 删除 | 4 | 210（3%） | Canal 直连模式的遗留代码 |

| 需要修改的文件（行数） | 改动点 |
| --- | --- |
| `CanalCacheSyncService`（868） | 分组改从注册表读取，按 `库名.表名` 匹配，新增主键改为广播，限制刷新策略 |
| `ArchiveStore`（582） | 按表的归属拆成归档、投递两部分 |
| `CachedQueryService`（483） | 新鲜度改读 Redis 信号；查询和回填逻辑不变 |
| `CdcHeartbeatMonitor`（400） | 起始行号可配置，发布新鲜度信号；检测算法不变 |
| `SubscriptionController`（214） | 订阅字段校验改用本地表结构 |
| `CacheAnnotationProcessor`（209） | 扫描和校验留在接入方，增加写注册表 |
| `PrimaryKeyFilters`（185） | 新增主键改从广播接收 |
| `TableRegistry`（159） | 拆成读取表结构元数据（公共）和表结构快照（归档） |
| `HistoryController`（150） | 消费者状态只报归档 |
| `ArchiveService`（147） | 去掉生成投递任务的部分，移到投递服务 |
| `PlatformConfiguration`、`PlatformProperties`（187） | 按服务拆分配置 |
| `QueryProtection`（110） | 参数改为可动态调整 |
| `ReplayService`（94） | 取历史事件改走 Feign |
| `AutoCacheConfig`（73） | 写入注册表时不带实体类 |

新增代码：注册表、新鲜度信号、布隆广播、投递消费者和去重表、Feign 客户端和内部接口、网关（路由、鉴权、签发令牌、限流）、各服务的启动类和配置、部署脚本。现有 52 个测试文件（7341 行）随代码迁移，修改过的类对应的测试要跟着改，新增的跨进程机制要补测试。

### 15.2 策略逐项对比

| 策略 | 原项目 | 拆分后 |
| --- | --- | --- |
| 缓存读取 | 批量读 Hash，只加载缺失的 ID | 不变，移到接入组件 |
| 防旧值回填 | 查库前写 `load_token`，失效时删除整个 Hash，回填时标识不符就放弃 | 不变。机制只依赖 Redis，跨进程照样成立 |
| 同 key 并发合并 | 同一实例内同一个 key 只查一次库 | 不变，仍限于单实例 |
| 查库限流 | 正常、窄两条通道，拿不到名额返回 503 | 逻辑不变，参数可在 Nacos 动态调整 |
| Redis 熔断 | 连续失败后熔断，期满放一个请求试探 | 不变 |
| 过期时间打散 | 随机增加 0～10% | 不变 |
| 二级索引缓存 | 列值对应主键列表，修改前后的列值各删一次 | 不变 |
| 主键布隆过滤器 | 启动时读出全部主键，CDC 新增在同进程补充 | 读库部分不变；新增主键改为 Redis 广播，多实例时每个实例都能收到全部新增 |
| 心跳降级 | 链路落后 3 秒，查询绕过缓存 | 判断逻辑不变；接入方经 Redis 信号得知，最长约 3.4 秒；新增同步服务挂掉时也绕过缓存 |
| 分区顺序处理 | 每个分区一个线程，整批确认，失败整批回滚重试 | 不变 |
| 删除策略、版本控制 | binlog 位置编码成版本号，Lua 脚本比较 | 不变 |
| 刷新、混合策略 | 同步服务把行数据转成实体类后写缓存 | 第一阶段不支持（第 6.9 节） |
| DDL 处理 | 清空、删除、改名表后整组失效 | 不变，匹配改为 `库名.表名` |
| 死信 | 确定无法处理的事件写入 Redis Stream | 不变 |
| 消息丢失检测 | 比较心跳行的修改前镜像和上次处理到的值 | 算法不变，心跳行号可配置 |
| 分组配置来源 | 扫描本进程的 `@AutoCache` | 改为 Redis 注册表，同步服务确认后接入方才启用缓存 |
| 归档 | 按源事件键去重，一次拉取一个事务 | 不变，去掉生成投递任务 |
| 表结构快照、恢复基线、恢复 SQL | — | 不变 |
| 订阅条件匹配 | 在归档事务里匹配 | 匹配逻辑不变，改在投递服务的消费者里执行 |
| 投递 | 租约领取、重试上限、尝试记录 | 不变 |
| 投递去重 | 依赖归档的源事件去重 | 投递服务自己的源事件去重表，算法相同 |
| 重放 | 指定最多 100 个事件，从归档表取出后重新投递 | 逻辑不变，取事件改走 Feign |
| 演示接收端 | 可注入故障的内置 Webhook | 不变 |

### 15.3 行为上的变化

- 变差：降级最长从 3 秒变为约 3.4 秒；刷新、混合策略暂时不能用（现有分组不受影响）。
- 新的边界情况：归档和投递相互独立，一条变更可能已经投递但还没归档，反之亦然；归档里没有的事件不能重放。
- 变好：同步服务挂掉时查询自动降级；归档故障不影响投递；投递接收端故障不影响缓存失效（第 7 步用压测证明）；多实例时布隆过滤器能收到全部新增主键；查询保护参数可以动态调整；管理接口有鉴权。
