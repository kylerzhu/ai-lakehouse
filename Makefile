SHELL := /bin/bash
.DEFAULT_GOAL := help

# 同时加载版本文件与密码文件；所有 docker compose 调用都通过这个变量
COMPOSE   := docker compose --env-file infra/versions.env --env-file .env
TOPICS    := llm.calls.raw llm.calls.payload
KAFKA_BIN := /opt/kafka/bin

help: ## 列出所有命令
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- 生命周期

env: ## 创建 .env 并生成随机密码（已存在则不覆盖）
	@if [ -f .env ]; then echo ".env 已存在，保持不变"; else \
	  pw=$$(openssl rand -hex 16); \
	  sed "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=$$pw|" .env.example > .env; \
	  echo "已创建 .env（Postgres 密码为随机生成）"; fi

build: env ## 构建自建的 Flink / Spark 镜像（jar 来自 infra/jars.txt，有缓存时很快）
	$(COMPOSE) --profile batch build

up: build ## 构建镜像并启动常驻服务（kafka / flink / postgres），等待全部 healthy
	$(COMPOSE) up -d --wait kafka flink-jobmanager flink-taskmanager postgres
	@$(MAKE) --no-print-directory status

down: ## 停止所有服务（数据保留）
	$(COMPOSE) --profile batch down

clean: ## 停止所有服务并删除数据卷（Kafka 消息、Paimon 表、Postgres 数据全部清空）
	$(COMPOSE) --profile batch down -v

status: ## 查看服务状态与健康检查
	@$(COMPOSE) ps --format 'table {{.Service}}\t{{.State}}\t{{.Status}}\t{{.Ports}}'

stats: ## 查看各容器的内存与 CPU 占用
	@docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.CPUPerc}}' $$($(COMPOSE) ps -q)

logs: ## 跟踪某个服务的日志：make logs S=flink-taskmanager
	@test -n "$(S)" || (echo "用法：make logs S=<服务名>"; exit 1)
	$(COMPOSE) logs -f --tail 100 $(S)

# ---------------------------------------------------------------- Kafka

topics: ## 创建 Kafka topic（llm.calls.raw / llm.calls.payload），已存在则跳过
	@for t in $(TOPICS); do \
	  $(COMPOSE) exec -T kafka $(KAFKA_BIN)/kafka-topics.sh --bootstrap-server localhost:9092 \
	    --create --if-not-exists --topic $$t --partitions 3 --replication-factor 1; \
	done
	@$(COMPOSE) exec -T kafka $(KAFKA_BIN)/kafka-topics.sh --bootstrap-server localhost:9092 --list

# ---------------------------------------------------------------- 检查

check-jars: ## 验证 infra/jars.txt 中所有 jar 的下载地址
	@./scripts/check_jars.sh

spark-version: ## 检查 Spark 容器能否启动（按需运行，结束后自动删除）
	$(COMPOSE) --profile batch run --rm --no-deps spark /opt/spark/bin/spark-submit --version

.PHONY: help env build up down clean status stats logs topics check-jars spark-version
