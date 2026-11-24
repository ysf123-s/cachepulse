# 多阶段构建：先用 Maven 打包过渡模块，再放进精简 JRE 镜像
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /workspace
COPY pom.xml .
COPY cachepulse-monolith/pom.xml cachepulse-monolith/
# 先单独拉依赖，利用 Docker 层缓存
RUN mvn -B -q -pl cachepulse-monolith -am dependency:go-offline
COPY cachepulse-monolith/src cachepulse-monolith/src
RUN mvn -B -q -pl cachepulse-monolith -am clean package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /workspace/cachepulse-monolith/target/cachepulse-monolith-0.1.0-SNAPSHOT.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
