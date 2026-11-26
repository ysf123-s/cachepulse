# Canal 1.1.8 空闲等待补丁

Canal 1.1.8 的 MQ 模式里，`CanalMQStarter` 每次拿到空批次都固定 `Thread.sleep(100)`。空闲时到达的变更要等它醒来，Canal 这一段平均多等约 50 ms，最多 100 ms，配置改不掉。

`CanalMQStarter.java` 是 1.1.8 原文件，只改了这一处：空批次后默认睡 1 ms，可用 JVM 参数 `-Dcanal.mq.idleSleepMs=<毫秒>` 调整（100 即原版行为）。配合 `canal.properties` 里的 `canal.mq.canalGetTimeout=1` 使用。

实测（`records/cachepulse-canal-tune-20261125/`，临时的第二个 Canal，每秒 700 次固定写入时负载相同）：

| 睡 / 等 / 批 | 空闲 CPU（一个核） | 空闲 P50 | 每秒 700 次 P50 | 每秒 700 次 P99 |
| --- | --- | --- | --- | --- |
| 100 / 5 / 16（原版） | 0.8% | 51 ms | 40 ms | 119 ms |
| 1 / 1 / 16（采用） | 2.5% | 1.8 ms | 7.3 ms | 46 ms |

## 构建和启动

在仓库根目录运行：

```bash
docker compose up -d --build
```

只构建 Canal 服务：

```bash
docker compose build canal
```

镜像构建使用 Canal 自带的 JDK 8 和依赖，编译后把 `CanalMQStarter` 的相关类写入镜像里的 `canal.server-1.1.8.jar`。Compose 挂载的 `canal.properties` 启用文件式实例配置，启动时加载 `example` 实例。

Instabox 当前的容器通过手工替换 JAR 使用同一补丁，回滚脚本是 `/data/cachepulse/canal-patch/rollback.sh`。使用构建产物替换该容器时需沿用现有配置目录，其中包含续读位点。
