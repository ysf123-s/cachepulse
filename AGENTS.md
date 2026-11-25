# AGENTS.md

## 来源与边界

- 本项目由 `C:\Users\Lenovo\Desktop\数据库变更处理与缓存同步平台`（下称原项目）迁入后改造。原项目保持不动：不要修改、构建或提交原项目里的任何文件，需要的代码复制过来再改。
- 原项目的 `AGENTS.md` 记录了服务器、部署脚本和压测脚本的细节，部署 CachePulse 前先参考它。

## 环境

- Windows，默认 shell 为 PowerShell 7；没有 bash、rg、docker。
- JDK 17（Temurin 17.0.17），Maven 3.9.9，Maven 镜像为阿里云，下载依赖不需要代理；访问 GitHub 需要走系统代理 `127.0.0.1:7897`。
- 文件以 LF 为主，修改时保持原文件的换行风格。
- 连接凭据在项目根目录 `application-local.yml`（已被 gitignore），读取时不要打印密码。

## 服务器

- 主环境是 Instabox，服务器上的路径、容器和管理命令见 `C:\Users\Lenovo\Desktop\文档\Instabox服务器信息与连接指南.md` 和原项目 AGENTS.md；2026-11-24 核实的版本和配置记在设计文档第 3 节。
- 本机开着 Clash Verge 的 TUN 模式（fake-ip）时，`ssh.instabox.sh` 会被解析成 198.18.x.x 的假地址而连不上。先用 `https://dns.alidns.com/resolve?name=ssh.instabox.sh&type=A` 查真实 IP，再用已知主机密钥连接：`ssh -p 2222 "-oHostKeyAlias=[ssh.instabox.sh]:2222" ibssh_nl2b6j2z5ucmekxqbnpamxl6vi@<真实IP>`。
- 中间件只监听 Instabox 本机，经 SSH 隧道访问，例如 `-L 16379:127.0.0.1:6379 -L 9092:127.0.0.1:9092`。Kafka 对外通告 `localhost:9092`，本地端口必须也是 9092。
- 阿里云 ECS（116.62.85.247）2026-11-24 从本机连不通：TCP 握手成功但收不到任何数据，原因未查明。
- Instabox 上 Canal 的实例配置以容器环境变量为准：过滤规则 `canal.instance.filter.regex=demo_db\..*`、MySQL 地址等都由环境变量覆盖了配置文件（配置文件里还写着 `.*\\..*`），用 `docker inspect canal-server` 查看，只看配置文件会误判。
- 开发期间不要在本机启用 CDC 心跳写入（设计文档第 6.5 节）；在服务器上执行会改动数据的操作前先征得用户同意。

## 用户偏好

- 项目要尽快完成用于面试：先把代码写出来，最后统一测试。
- 没有用户明确要求时不要提交。

## 设计

- 拆分设计在 `docs/design/microservice-split.md`。设计有变化时先改文档再改代码。
- 服务间调用的超时只在 Feign 里配置，并关闭 Spring Cloud CircuitBreaker 的 TimeLimiter 和线程池（`spring.cloud.circuitbreaker.resilience4j.disable-time-limiter=true`、`disable-thread-pool=true`）：默认的 TimeLimiter 只给 1 秒，会覆盖 Feign 的读超时。网关路由的熔断同样默认 1 秒，要按路由显式配置。

## 验证命令

- Java 全量测试：`mvn -o -B test`（新增依赖后先去掉 `-o` 联网跑一次）。需要外部 Redis、MySQL、Docker 的测试会自动跳过，目前共 430 项，跳过 38 项（2026-11-25）。本机内存紧张，几个进程同时跑 Maven 时要排队，不要并行。
- 管理页面脚本测试：在 `gateway/` 目录下执行 `node src/test/js/cache-dashboard.test.cjs`、`node src/test/js/platform-dashboard.test.cjs`。
- 真实 Redis 测试：`mvn -o -B test "-Dtest=*RedisTest" "-Dsurefire.failIfNoSpecifiedTests=false" -Dcache.test.redis.host=<host>`，端口和密码取环境变量 `CACHE_TEST_REDIS_PORT`、`CACHE_TEST_REDIS_PASSWORD`（从 `application-local.yml` 的 `spring.data.redis` 解析）；只使用随机前缀 key 并自行清理。

## 结构约定

- 原单体（过渡模块 `cachepulse-monolith`）已在 2026-11-25 删除，测试已迁到各模块；要看原来的代码用 `git show HEAD:cachepulse-monolith/...`（第一个提交）。
- 读取仓库根目录文件（`docker/`、`examples/` 下的 SQL）的模块，surefire 的工作目录设为仓库根目录，见各模块 pom。
- 版本统一在根 `pom.xml` 管理。kafka-clients 跟随 Spring Boot 管理的 3.9.x，不要升到 4.x（原因见根 pom 注释）；Canal 带进来的 protobuf-java 统一为 3.25.9，ZooKeeper 已排除。
- 本机验证 Nacos：用官方包 `nacos-server-3.0.3.zip`，直接 `java -jar target/nacos-server.jar`（参数照 `bin/startup.cmd`，PowerShell 里每个 `-D` 参数都要加引号）。开启鉴权要给 `nacos.core.auth.plugin.nacos.token.secret.key`（Base64，原文至少 32 字节）和 `nacos.core.auth.server.identity.key/value`，启动后 `POST /nacos/v3/auth/user/admin` 初始化管理员密码。
- 服务间调用要设 `spring.cloud.loadbalancer.cache.enabled=false`：Nacos 客户端本身有推送更新的实例缓存，LoadBalancer 默认再缓存 35 秒，实测被调方下线后还会往旧实例发约 23 秒请求。
- `records/`（只在本地，已忽略）存放过程记录、部署和压测记录。
- 根目录的 `application-local.yml` 是原项目的配置（指向 `cdc_platform` 和原项目的消费组），CachePulse 的服务不读它、不启用 `local` profile。本机覆盖写在 `cachepulse-local.yml`，密码写在 `cachepulse-local.env`（都已忽略），启停用 `scripts/local/start.ps1`、`stop.ps1`。
- Nacos 配置的源文件在 `config/nacos/`，改了以后用 `scripts/local/nacos-publish.ps1` 发布；密码不进 Nacos。
