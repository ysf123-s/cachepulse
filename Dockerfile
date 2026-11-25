# 构建单个服务的镜像：docker build --build-arg SERVICE=gateway -t cachepulse/gateway .
# SERVICE 取 gateway、cache-sync-service、archive-service、delivery-service、demo-user-service、demo-inventory-service。
# Instabox 上不建镜像（系统盘紧张），直接用 JDK 17 镜像只读挂载 JAR，见设计文档第 10.1 节。
FROM maven:3.9-eclipse-temurin-17 AS build
ARG SERVICE
WORKDIR /workspace
COPY . .
RUN test -n "$SERVICE" && mvn -B -q -pl "$SERVICE" -am clean package -DskipTests \
    && cp "$SERVICE/target/$SERVICE-0.1.0-SNAPSHOT.jar" /workspace/app.jar

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /workspace/app.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
