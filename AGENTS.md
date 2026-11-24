# AGENTS.md

## 来源与边界

- 本项目由 `C:\Users\Lenovo\Desktop\数据库变更处理与缓存同步平台`（下称原项目）迁入后改造。原项目保持不动：不要修改、构建或提交原项目里的任何文件，需要的代码复制过来再改。
- 原项目的 `AGENTS.md` 记录了服务器、部署脚本和压测脚本的细节，部署 CachePulse 前先参考它。

## 环境

- Windows，默认 shell 为 PowerShell 7；没有 bash、rg、docker。
- JDK 17（Temurin 17.0.17），Maven 3.9.9，Maven 镜像为阿里云，下载依赖不需要代理；访问 GitHub 需要走系统代理 `127.0.0.1:7897`。
- 文件以 LF 为主，修改时保持原文件的换行风格。
- 连接凭据在项目根目录 `application-local.yml`（已被 gitignore），读取时不要打印密码。

## 验证命令

- Java 全量测试：`mvn -o -B test`（新增依赖后先去掉 `-o` 联网跑一次）。需要外部 Redis、MySQL、Docker 的测试会自动跳过，目前共 360 项，跳过 42 项。
- 管理页面脚本测试：在 `cachepulse-monolith/` 目录下执行 `node src/test/js/cache-dashboard.test.cjs`、`node src/test/js/platform-dashboard.test.cjs`。
- 真实 Redis 测试：`mvn -o -B test "-Dtest=*RedisTest" "-Dsurefire.failIfNoSpecifiedTests=false" -Dcache.test.redis.host=<host>`，端口和密码取环境变量 `CACHE_TEST_REDIS_PORT`、`CACHE_TEST_REDIS_PASSWORD`（从 `application-local.yml` 的 `spring.data.redis` 解析）；只使用随机前缀 key 并自行清理。

## 结构约定

- 过渡模块 `cachepulse-monolith` 的测试以仓库根目录为工作目录（读取 `docker/`、`examples/` 下的 SQL），已在模块 pom 的 surefire 配置里设置。
- 版本统一在根 `pom.xml` 管理；`kafka.version` 固定为 3.6.2，不跟随 Spring Boot 升级。
- `records/`（只在本地，已忽略）存放过程记录、部署和压测记录。
