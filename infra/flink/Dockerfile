# Flink 1.20 + Paimon + Kafka / JDBC connector + Hadoop 依赖
# 构建上下文是 infra/（见 docker-compose.yml），jar 清单来自 infra/jars.txt。
ARG FLINK_IMAGE=flink:1.20.5-scala_2.12-java17
FROM ${FLINK_IMAGE}

COPY jars.txt fetch_jars.sh /tmp/
RUN sh /tmp/fetch_jars.sh flink /opt/flink/lib /tmp/jars.txt \
 && chown flink:flink /opt/flink/lib/*.jar \
 && rm /tmp/jars.txt /tmp/fetch_jars.sh

# Flink SQL 作业文件（T07 起使用）
COPY flink/sql/ /opt/sql/
