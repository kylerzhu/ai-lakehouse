# LLM 调用可观测性与评测平台：落地实施文档

> 本文是 `DESIGN.md` 的可执行版本：修正了原设计中的 SQL 与埋点问题，按 MacBook Air（M2 / 8GB）的资源做了裁剪，并把每个阶段拆成可验收的任务。
> 原则：**先跑通最小链路，再逐步加功能；每一步都有可验证的产出。**

---

## 0. 一句话说明

做一个小型 Agent，给它的模型调用和工具调用包一层埋点；埋点事件和模拟器造的事件一起进入 Kafka，经 Flink（实时）和 Spark（批处理）写入 Paimon 数据湖并分层建模，产出成本、失败、延迟指标和告警；另有一条离线评测链路，对比不同提示词版本，并与线上指标关联。

```
┌──────────────┐   ┌──────────────┐
│ 小型 Agent    │   │ 模拟器        │  ← 两者产出格式完全相同的事件
│ + 埋点封装    │   │ (造数/注入异常)│
└──────┬───────┘   └──────┬───────┘
       └────────┬─────────┘
                ▼
        Kafka: llm.calls.raw
                │
     ┌──────────┴───────────┐
     ▼                      ▼
Flink（常驻）              Spark（按需，每天一次）
 ODS/DWD 入湖               trace 粒度汇总
 分钟级聚合 + 告警           天级精确汇总 → ADS
     │                      │
     └──────────┬───────────┘
                ▼
     Paimon（本地目录 warehouse）
                ▼
     PostgreSQL（告警表 / ADS） → Streamlit 看板

离线评测：测试集 → Agent(prompt vN) → eval_run / eval_case → 版本 diff → 按 prompt_version 关联线上
```

---

## 1. 运行环境与资源约束

### 1.1 机器

- MacBook Air M2，8GB 内存，arm64；可用磁盘约 70GB
- 容器运行时：**OrbStack**（推荐，比 Docker Desktop 省内存）或 Docker Desktop
- 容器内存上限设置为 **5 到 6GB**
- Python：用 `uv` 建项目虚拟环境（Python 3.11），不使用 anaconda 的 base 环境

### 1.2 内存预算（硬性约束）

| 服务 | 运行方式 | 内存上限 (`mem_limit`) |
|---|---|---|
| Kafka（KRaft 单节点，heap 384m） | 常驻 | 768m |
| Flink JobManager | 常驻 | 1g |
| Flink TaskManager（2 slots） | 常驻 | 1600m |
| PostgreSQL 16 | 常驻 | 512m |
| Spark（`local[2]`，driver 1g） | **按需 `docker compose run --rm`，跑完退出** | 2g |
| 模拟器、Agent、Streamlit | 宿主机 Python 进程 | — |

规则：
1. **Spark 批处理和压测不同时进行**；跑 Spark 时可以暂停模拟器。
2. **不在本地跑大模型**。Agent 调用 OpenAI 兼容 API（DeepSeek / 通义 / gpt-4o-mini 等）。
3. 不用时执行 `make down` 并退出容器运行时，释放全部内存。
4. Kafka 设置 `KAFKA_LOG_RETENTION_HOURS=24`，避免磁盘持续增长。

### 1.2.1 端口（全部只绑定 127.0.0.1）

| 服务 | 宿主机地址 | 容器内地址 |
|---|---|---|
| Kafka | `localhost:9094` | `kafka:9092` |
| Flink Web UI / REST | `localhost:8081` | `flink-jobmanager:8081` |
| PostgreSQL | `localhost:5433`（本机 5432 已被占用） | `postgres:5432` |
| Spark UI | `localhost:4040`（仅作业运行时） | — |

Flink 与 Spark 都以 uid 9999 运行，共享 `paimon-data` 卷（挂载到 `/data/paimon`）；卷的所有者由一次性容器 `volume-init` 设置。

### 1.3 版本选择

已于 2026-11-19 核对（T01）。版本的唯一来源是 [`infra/versions.env`](infra/versions.env)，jar 清单在 [`infra/jars.txt`](infra/jars.txt)，用 `scripts/check_jars.sh` 验证下载地址。

| 组件 | 确定版本 | 说明 |
|---|---|---|
| Flink 镜像 | `flink:1.20.5-scala_2.12-java17` | 1.20 系列最新补丁版，有 arm64 |
| Paimon | **1.4.2** | `paimon-flink-1.20-1.4.2.jar` 与 `paimon-spark-3.5_2.12-1.4.2.jar`，两边版本一致 |
| Hadoop 依赖（Flink） | `flink-shaded-hadoop-2-uber-2.8.3-10.0.jar` | Paimon 1.4 文档要求：没有 Hadoop 环境时，Flink 的 lib 目录需要放这个预打包 jar；Spark 镜像自带 Hadoop，不需要 |
| Flink Kafka connector | `flink-sql-connector-kafka-3.4.0-1.20` | 1.20 对应的最新版 |
| Flink JDBC connector | `flink-connector-jdbc-3.3.0-1.20` | 单 jar，包含 Postgres 方言；告警写入 Postgres |
| PostgreSQL JDBC 驱动 | `postgresql-42.7.14` | Flink 与 Spark 共用 |
| Spark 镜像 | `apache/spark:3.5.9-scala2.12-java17-python3-ubuntu` | 带 Python，可直接跑 PySpark；Scala 2.12 与 Paimon jar 对应 |
| Kafka 镜像 | `apache/kafka:4.3.2` | KRaft 模式；4.x broker 兼容 connector 使用的 3.x 客户端 |
| PostgreSQL 镜像 | `postgres:16.15` | |

**选型说明**

- **Paimon 选 1.4.2 而不是最新的 2.1.0。** 2.x 是大版本升级，官方文档结构也重组了，Flink / Spark 的快速开始步骤不如 1.4 的完整；1.4.2 发布于 2026 年 6 月，文档里有逐步的部署说明，适合第一次接触 Paimon。本项目用到的主键表、`deduplicate` 合并引擎、`changelog-producer`、lookup join 在 1.4 中都已成熟。Phase 5 之后可以评估升级。
- **Flink 留在 1.20 而不是 2.x。** 1.20 的社区资料和示例最多，Paimon 与各 connector 对它的支持最稳定。
- **JDBC connector 选 3.3.0 而不是 3.4.0。** 3.4.0 开始拆分为 core 与各数据库方言的独立 jar，3.3.0 单 jar 更简单。
- 所有镜像都写死到补丁版本，不使用 `latest` 或主版本号标签。

---

## 2. 目录结构

```
data_lake/
├── Claude.md                    # 本文件
├── README.md                    # Phase 5 编写：架构、取舍、运行方式、实测数字
├── Makefile
├── docker-compose.yml
├── .env.example                 # API Key、模型名、Postgres 密码（.env 不入库）
├── pyproject.toml
├── infra/
│   ├── flink/
│   │   ├── Dockerfile           # 基于官方镜像，wget 下载 Paimon/Kafka/JDBC jar
│   │   └── sql/
│   │       ├── 00_catalog.sql
│   │       ├── 01_dim.sql
│   │       ├── 02_ods_dwd.sql
│   │       └── 03_dws_alert.sql
│   ├── spark/
│   │   ├── Dockerfile           # 基于 apache/spark，加 paimon-spark jar
│   │   └── spark-defaults.conf
│   └── postgres/
│       └── init.sql             # alert_event、ads_*、eval_* 表
├── src/llmobs/
│   ├── instrumentation/         # 埋点（核心）
│   │   ├── context.py           # contextvars：trace / span 上下文
│   │   ├── schema.py            # 事件 dataclass + 校验
│   │   ├── producer.py          # 异步 Kafka producer
│   │   ├── client.py            # ObservedLLMClient
│   │   └── tools.py             # @traced_tool / @traced_retrieval 装饰器
│   ├── agent/                   # 小型 text-to-SQL Agent
│   │   ├── prompts/             # v1.md、v2.md ……（prompt_version 即文件名）
│   │   ├── tools.py             # list_tables / describe_table / run_sql
│   │   └── run.py
│   ├── simulator/               # 造数与异常注入
│   │   ├── profiles.py          # 各 feature 的步数、token、延迟分布
│   │   ├── anomalies.py         # 死循环、token 暴涨、失败率、重复、乱序
│   │   └── run.py
│   ├── batch/                   # Spark 作业（PySpark + SQL）
│   │   ├── dws_trace_1d.py
│   │   ├── dws_feature_1d.py
│   │   ├── reprice.py           # 价格维表变更后重算历史
│   │   └── export_ads.py        # 写 Postgres
│   ├── eval/
│   │   ├── run_eval.py          # 对指定 prompt_version 跑测试集
│   │   ├── judge.py             # LLM-as-judge + 一致率校准
│   │   └── regress.py           # 两个 run 的 case 级 diff
│   └── dashboard/app.py         # Streamlit
├── data/                        # 不入库：评测数据库、测试集、公开 trace
└── tests/
```

---

## 3. 事件 Schema（修订版）

相对 `DESIGN.md` 的改动：增加 `schema_version`、`attempt`；`retry_count` 的含义改为"本次逻辑调用的第几次重试"；字段命名尽量与 OpenTelemetry GenAI 语义约定对应，便于以后迁移。

```json
{
  "schema_version": 1,
  "trace_id":       "tr_8f3a...",          // 一次完整任务，必填
  "span_id":        "sp_01c2...",          // 本次调用，必填且全局唯一（uuid4）
  "parent_span_id": "sp_00a1...",          // 根节点为 null
  "span_type":      "llm | tool | retrieval",

  "ts_start":       "2026-11-19T10:00:00.123Z",
  "ts_end":         "2026-11-19T10:00:02.456Z",
  "latency_ms":     2333,

  "feature":        "text_to_sql",         // 成本归因主维度
  "model":          "deepseek-chat",       // tool/retrieval 为 null
  "prompt_version": "v2",
  "tool_name":      "run_sql",             // span_type=tool 时填

  "input_tokens":   1520,                  // ↔ gen_ai.usage.input_tokens
  "output_tokens":  340,                   // ↔ gen_ai.usage.output_tokens
  "cached_tokens":  800,

  "status":         "success | failed | timeout",
  "error_type":     "rate_limit | timeout | invalid_output | tool_error | api_error | null",
  "attempt":        1,                     // 第几次尝试，从 1 开始；每次尝试是一个独立 span
  "retry_count":    0,                     // = attempt - 1
  "cache_hit":      false,

  "prompt_hash":    "sha256:...",
  "user_id_hash":   "u_7d2e...",
  "env":            "prod | dev",
  "source":         "agent | simulator"    // 便于区分真实与模拟数据
}
```

约定：
- **事件里不写成本**，只写 token；成本在 DWD 层用价格维表计算。
- 完整 prompt / 输出不进主事件；失败全量、成功按 1% 抽样，发到单独的 topic `llm.calls.payload`，落 `dwd_llm_payload`（TTL 7 天）。
- 价格单位统一为 **美元 / 百万 token**。

---

## 4. 埋点实现

### 4.1 设计要点

1. **埋点集中在封装层**，Agent 业务代码只调用封装，不直接写埋点。
2. 用 `contextvars` 自动传递 trace_id 与当前 span_id，子调用自动获得 `parent_span_id`。
3. **关闭 SDK 内置重试**（`max_retries=0`），由封装自己重试，每次尝试记一个 span。否则 SDK 的静默重试不可见，重试率永远是 0。
4. 在 `finally` 中发送事件，失败调用必须记录；捕获**所有**异常并映射 `error_type`。
5. 使用异步 producer，发送失败只记日志，不影响业务。

### 4.2 参考代码骨架

```python
# instrumentation/context.py
import contextvars, uuid
_trace_id = contextvars.ContextVar("trace_id", default=None)
_span_id  = contextvars.ContextVar("span_id",  default=None)

def new_id(prefix): return f"{prefix}_{uuid.uuid4().hex}"

class trace:
    """with trace(feature=..., prompt_version=...): 开启一次任务"""
    def __init__(self, **attrs): self.attrs = attrs
    def __enter__(self):
        self.tok = _trace_id.set(new_id("tr"))
        return self
    def __exit__(self, *a): _trace_id.reset(self.tok)
```

```python
# instrumentation/client.py（核心逻辑）
import time, openai
from .context import _trace_id, _span_id, new_id
from .producer import emit

ERROR_MAP = {
    openai.RateLimitError: "rate_limit",
    openai.APITimeoutError: "timeout",
    openai.APIError: "api_error",
}

class ObservedLLMClient:
    def __init__(self, base_url, api_key, model, feature, max_attempts=3):
        self.client = openai.OpenAI(base_url=base_url, api_key=api_key, max_retries=0)  # 关闭静默重试
        self.model, self.feature, self.max_attempts = model, feature, max_attempts

    def chat(self, messages, prompt_version, **kw):
        for attempt in range(1, self.max_attempts + 1):
            span_id, parent = new_id("sp"), _span_id.get()
            tok = _span_id.set(span_id)
            t0 = time.time()
            status, err, resp = "success", None, None
            try:
                resp = self.client.chat.completions.create(model=self.model, messages=messages, **kw)
                return resp
            except Exception as e:
                status = "timeout" if isinstance(e, openai.APITimeoutError) else "failed"
                err = next((v for k, v in ERROR_MAP.items() if isinstance(e, k)), "api_error")
                if attempt == self.max_attempts or err not in ("rate_limit", "timeout"):
                    raise
                time.sleep(2 ** attempt)          # 指数退避后重试
            finally:
                _span_id.reset(tok)
                usage = getattr(resp, "usage", None)
                emit(span_type="llm", trace_id=_trace_id.get(), span_id=span_id,
                     parent_span_id=parent, feature=self.feature, model=self.model,
                     prompt_version=prompt_version, ts_start=t0, ts_end=time.time(),
                     input_tokens=getattr(usage, "prompt_tokens", 0) or 0,
                     output_tokens=getattr(usage, "completion_tokens", 0) or 0,
                     cached_tokens=_cached(usage), status=status, error_type=err,
                     attempt=attempt, retry_count=attempt - 1, messages=messages)
```

```python
# instrumentation/tools.py：工具调用同样产出 span
def traced_tool(fn):
    def wrapper(*args, **kwargs):
        # 与 chat 相同：新 span_id、记录 parent、计时、try/except/finally emit(span_type="tool", tool_name=fn.__name__)
        ...
    return wrapper
```

```python
# instrumentation/producer.py
from confluent_kafka import Producer
_p = Producer({"bootstrap.servers": "localhost:9094", "linger.ms": 20, "enable.idempotence": True})
def emit(**fields):
    event = build_event(**fields)           # schema.py：补默认字段、算 latency、hash prompt、校验
    try:
        _p.produce("llm.calls.raw", key=event["trace_id"], value=json.dumps(event), on_delivery=_log_err)
        _p.poll(0)
    except Exception:
        log.exception("emit failed")        # 永不抛出
```

> Kafka 的 key 用 `trace_id`：同一任务的事件进同一分区，保持相对有序。
> 宿主机连接 Kafka 需要在 compose 中配置一个 EXTERNAL listener（如 `localhost:9094`），容器内部用 `kafka:9092`。

### 4.3 后续升级路径（可选）

- 接 LangChain / LangGraph：写一个 `BaseCallbackHandler`，把 `run_id` / `parent_run_id` 映射为 span / parent，复用 `emit()`。
- 接 OpenTelemetry：字段已与 GenAI 语义约定对齐，可改为 OTel SDK 产出 span，经 OTel Collector 的 Kafka exporter 写入同一 topic。

---

## 5. 数据来源

| 来源 | 作用 | 规模 |
|---|---|---|
| **小型 Agent**（真实调用，`source=agent`） | 证明埋点在真实场景可用；评测被测对象 | 几千条 |
| **模拟器**（`source=simulator`） | 数据量与压测；异常注入 | 几百万条（历史回灌）+ 实时 QPS 可配 |
| **公开 LLM trace**（BurstGPT / Azure LLM Inference Trace） | 校准模拟器的到达间隔与 token 分布 | 只读取分布，不直接入湖 |
| **评测集**（Spider / BIRD 子集） | 离线评测 | 100 到 200 条 + 50 条人工标注 |

### 5.1 小型 Agent：text-to-SQL

- 任务：用户提问 → Agent 查表结构 → 写 SQL → 执行 → 用自然语言回答。
- 数据库：从 Spider（或 BIRD mini-dev）中选 2 到 3 个 SQLite 数据库，放在 `data/db/`。
- 工具：`list_tables`、`describe_table`、`run_sql`（只读，拒绝 DDL/DML）。
- 循环：function calling，最多 6 步；超出步数记为失败（`error_type=invalid_output`）。
- 提示词：`agent/prompts/v1.md`、`v2.md`，`prompt_version` 即文件名。
- 选这个任务的原因：答案可以自动判对错（执行结果比对），评测不完全依赖 judge。

### 5.2 模拟器

参照参考项目 `sources/iot.py` 的写法，但产出 trace 结构：

- **feature 画像**（`profiles.py`）：至少 3 个 feature，例如 `text_to_sql`、`report_generation`、`customer_support`，各自有步数分布（3 到 8 步）、llm/tool 比例、token 分布（对数正态，参数用公开 trace 校准）、延迟分布、基础失败率。
- **时间戳**：按 trace 内 span 依次生成，父子关系合理。
- **两种模式**：
  - `backfill --days 30 --qps 50`：生成历史数据，`ts` 落在过去，用于批处理与看板。
  - `live --qps 20`：实时生成，用于 Flink 与告警。
- **异常注入**（`anomalies.py`，命令行开关）：

| 开关 | 效果 | 验证目标 |
|---|---|---|
| `--inject loop` | 某个 trace 连续 50 次 llm 调用 | 成本突增告警 |
| `--inject token-spike` | 某 feature 的 input_tokens × 10 | 成本突增告警 |
| `--inject fail-rate` | 某 feature 失败率升到 30% | 失败率告警 |
| `--inject retry-storm` | 大量 `attempt>1` | 重试风暴告警 |
| `--dup-rate 0.01` | 1% 事件重复发送 | DWD 去重 |
| `--late-rate 0.01 --late-sec 30` | 1% 事件延迟 30 秒发送 | 乱序 / watermark |
| `--bad-rate 0.01` | 1% 事件缺字段或字段非法 | 清洗层过滤 |

---

## 6. 分层模型

| 层 | 表 | 位置 | 粒度 | 写入方 |
|---|---|---|---|---|
| DIM | `dim_model_price_hist` | Paimon | 模型 × 生效日期 | 手工 / 脚本 |
| DIM | `dim_model_price_current` | Paimon | 模型 | 由 hist 派生，供 Flink lookup join |
| ODS | `ods_llm_call` | Paimon（append 表） | 一次调用（原样） | Flink |
| DWD | `dwd_llm_call` | Paimon（主键表） | 一次调用 | Flink；`reprice.py` 可重算 |
| DWD | `dwd_llm_payload` | Paimon（TTL 7 天） | 抽样调用 | Flink |
| DWS | `dws_llm_feature_1min` | Paimon + Postgres | feature × model × 分钟 | Flink（近似值） |
| DWS | `dws_llm_feature_1d` | Paimon | feature × model × 天 | Spark（精确值） |
| DWS | `dws_llm_trace_1d` | Paimon | 一次任务 | Spark（**核心表**） |
| ADS | `ads_cost_daily` / `ads_trace_daily` / `ads_eval_vs_online` | Postgres | 看板维度 | Spark |
| — | `alert_event` | Postgres | 一条告警 | Flink |
| — | `eval_run` / `eval_case` / `eval_regress` | Postgres | 评测 | Python |

> 价格维表拆成两张：Flink 的 lookup join 按 `model` 关联，如果维表里同一模型有多个生效日期，会关联出多行导致重复。所以实时链路用 `current`（每个模型一行），批处理重算用 `hist`（按时间区间关联）。
>
> **流与批的关系**：Flink 产出分钟级近似值（流式未去重、可能有迟到丢失），Spark 每天基于去重后的 DWD 全量重算精确值。看板的天级账目一律读 Spark 结果。

---

## 7. 实时链路：Flink SQL（修订版）

### 7.1 Catalog 与维表（`00_catalog.sql`、`01_dim.sql`）

```sql
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse' = 'file:///data/paimon');
CREATE DATABASE IF NOT EXISTS paimon.ops;

CREATE TABLE IF NOT EXISTS paimon.ops.dim_model_price_hist (
  model STRING, effective_from DATE, effective_to DATE,      -- effective_to 为 NULL 表示当前价格
  input_price DECIMAL(10,4), cached_price DECIMAL(10,4), output_price DECIMAL(10,4),  -- USD / 1M tokens
  PRIMARY KEY (model, effective_from) NOT ENFORCED
);

CREATE TABLE IF NOT EXISTS paimon.ops.dim_model_price_current (
  model STRING, input_price DECIMAL(10,4), cached_price DECIMAL(10,4), output_price DECIMAL(10,4),
  PRIMARY KEY (model) NOT ENFORCED
);
```

### 7.2 Kafka 源表与清洗视图（`02_ods_dwd.sql`）

```sql
CREATE TEMPORARY TABLE kafka_llm_call (
  schema_version INT, trace_id STRING, span_id STRING, parent_span_id STRING, span_type STRING,
  ts_start TIMESTAMP_LTZ(3), ts_end TIMESTAMP_LTZ(3), latency_ms INT,
  feature STRING, model STRING, prompt_version STRING, tool_name STRING,
  input_tokens INT, output_tokens INT, cached_tokens INT,
  status STRING, error_type STRING, attempt INT, retry_count INT, cache_hit BOOLEAN,
  prompt_hash STRING, user_id_hash STRING, env STRING, source STRING,
  proc_time AS PROCTIME(),                                        -- 修正：lookup join 需要
  WATERMARK FOR ts_end AS ts_end - INTERVAL '30' SECOND           -- 修正：容忍 30 秒乱序
) WITH (
  'connector' = 'kafka',
  'topic' = 'llm.calls.raw',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-llm-obs',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- 清洗 + 成本计算，ODS/DWD/DWS 共用
CREATE TEMPORARY VIEW v_call_enriched AS
SELECT
  c.*,
  DATE_FORMAT(c.ts_end, 'yyyy-MM-dd')                                        AS dt,
  COALESCE(c.input_tokens, 0) - COALESCE(c.cached_tokens, 0)                 AS billable_tokens,
  CAST(
    ( (COALESCE(c.input_tokens,0) - COALESCE(c.cached_tokens,0)) * p.input_price
      + COALESCE(c.cached_tokens,0) * p.cached_price
      + COALESCE(c.output_tokens,0) * p.output_price ) / 1000000
  AS DECIMAL(14,8))                                                          AS cost_usd
FROM kafka_llm_call AS c
LEFT JOIN paimon.ops.dim_model_price_current FOR SYSTEM_TIME AS OF c.proc_time AS p
  ON c.model = p.model
WHERE c.span_id IS NOT NULL AND c.trace_id IS NOT NULL
  AND c.ts_start IS NOT NULL AND c.ts_end IS NOT NULL
  AND c.span_type IN ('llm', 'tool', 'retrieval');
```

DWD 主键表：

```sql
CREATE TABLE IF NOT EXISTS paimon.ops.dwd_llm_call (
  dt STRING, span_id STRING, trace_id STRING, parent_span_id STRING, span_type STRING,
  ts_start TIMESTAMP_LTZ(3), ts_end TIMESTAMP_LTZ(3), latency_ms INT,     -- 修正：补 ts_start
  feature STRING, model STRING, prompt_version STRING, tool_name STRING,
  input_tokens INT, output_tokens INT, cached_tokens INT, billable_tokens INT,
  cost_usd DECIMAL(14,8),
  status STRING, error_type STRING, attempt INT, retry_count INT, cache_hit BOOLEAN,
  env STRING, source STRING,
  PRIMARY KEY (dt, span_id) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
  'bucket' = '2',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'input'
);
```

> 去重前提：重复事件的 `ts_end` 相同，因此 `dt` 相同，主键 `(dt, span_id)` 相同，Paimon 只保留最新一条。
> 本地数据量小，`bucket` 设为 2 即可，太多会产生大量小文件。

### 7.3 分钟级聚合与告警（`03_dws_alert.sql`）

```sql
CREATE TABLE IF NOT EXISTS paimon.ops.dws_llm_feature_1min (
  window_start TIMESTAMP(3), window_end TIMESTAMP(3), feature STRING, model STRING,
  call_cnt BIGINT, cost_usd DECIMAL(18,8), total_tokens BIGINT,
  fail_cnt BIGINT, retry_cnt BIGINT, avg_latency_ms DOUBLE, max_latency_ms INT,
  PRIMARY KEY (window_start, feature, model) NOT ENFORCED
);

CREATE TEMPORARY VIEW v_feature_1min AS
SELECT window_start, window_end, window_time, feature,
       COALESCE(model, '_tool') AS model,
       COUNT(*)                                             AS call_cnt,
       SUM(cost_usd)                                        AS cost_usd,
       SUM(COALESCE(input_tokens,0) + COALESCE(output_tokens,0)) AS total_tokens,
       SUM(CASE WHEN status <> 'success' THEN 1 ELSE 0 END) AS fail_cnt,
       SUM(CASE WHEN attempt > 1 THEN 1 ELSE 0 END)         AS retry_cnt,
       AVG(CAST(latency_ms AS DOUBLE))                      AS avg_latency_ms,
       MAX(latency_ms)                                      AS max_latency_ms
FROM TABLE(TUMBLE(TABLE v_call_enriched, DESCRIPTOR(ts_end), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, window_time, feature, COALESCE(model, '_tool');
```

> **p95 的处理**：Flink SQL 没有内置的近似分位数函数（`APPROX_PERCENTILE` 是 Spark 的）。实时链路用 `avg` / `max` 做延迟告警，p50/p95/p99 放到 Spark 批处理里用 `percentile_approx` 精确计算。如需实时 p95，再写 Java UDAF（扩展项）。

告警：在分钟聚合上叠加过去 60 分钟的 OVER 窗口作为基线。

```sql
CREATE TEMPORARY TABLE pg_alert_event (
  alert_time TIMESTAMP(3), rule STRING, feature STRING, model STRING,
  observed DOUBLE, baseline DOUBLE, detail STRING
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:postgresql://postgres:5432/llmobs',
  'table-name' = 'alert_event',
  'username' = '${PG_USER}', 'password' = '${PG_PASSWORD}'
);

CREATE TEMPORARY VIEW v_feature_1min_baseline AS
SELECT *,
  AVG(CAST(cost_usd AS DOUBLE)) OVER w AS cost_avg_1h,
  AVG(avg_latency_ms)           OVER w AS latency_avg_1h,
  COUNT(*)                      OVER w AS baseline_points
FROM v_feature_1min
WINDOW w AS (PARTITION BY feature, model ORDER BY window_time
             RANGE BETWEEN INTERVAL '60' MINUTE PRECEDING AND CURRENT ROW);

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO paimon.ops.ods_llm_call SELECT ... FROM kafka_llm_call;            -- 原样入 ODS
  INSERT INTO paimon.ops.dwd_llm_call SELECT ... FROM v_call_enriched;            -- DWD
  INSERT INTO paimon.ops.dws_llm_feature_1min SELECT ... FROM v_feature_1min;     -- DWS
  INSERT INTO pg_alert_event                                                      -- 告警
  SELECT window_end, rule, feature, model, observed, baseline, detail FROM (
    SELECT window_end, 'cost_spike' AS rule, feature, model,
           CAST(cost_usd AS DOUBLE) AS observed, cost_avg_1h AS baseline, '' AS detail
    FROM v_feature_1min_baseline
    WHERE baseline_points >= 10 AND CAST(cost_usd AS DOUBLE) > cost_avg_1h * 5
    UNION ALL
    SELECT window_end, 'fail_rate', feature, model, fail_cnt * 1.0 / call_cnt, 0.1, ''
    FROM v_feature_1min_baseline WHERE call_cnt > 20 AND fail_cnt * 1.0 / call_cnt > 0.1
    UNION ALL
    SELECT window_end, 'retry_storm', feature, model, retry_cnt * 1.0 / call_cnt, 0.3, ''
    FROM v_feature_1min_baseline WHERE call_cnt > 20 AND retry_cnt * 1.0 / call_cnt > 0.3
    UNION ALL
    SELECT window_end, 'latency_degrade', feature, model, avg_latency_ms, latency_avg_1h, ''
    FROM v_feature_1min_baseline WHERE baseline_points >= 10 AND avg_latency_ms > latency_avg_1h * 2
  );
END;
```

注意：
- 基线窗口包含当前分钟，会略微拉高均值；倍数阈值已足够宽，可接受。调试时可改为 `ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING`。
- **退路**：如果 OVER 窗口在你的 Flink 版本上调试困难，改为 Flink 只把 `dws_llm_feature_1min` 同步写入 Postgres，告警由一个 Python 进程每分钟查询判断。README 里说明这个取舍。
- `${...}` 占位符由 `submit.sh` 用 `envsubst` 从环境变量替换（参考项目 `infra/flink/submit.sh` 的做法），SQL 文件中不写明文密码。

---

## 8. 批处理链路：Spark（修订版）

Spark 配置（`spark-defaults.conf`）：

```
spark.sql.catalog.paimon            org.apache.paimon.spark.SparkCatalog
spark.sql.catalog.paimon.warehouse  file:/data/paimon
spark.sql.extensions                org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions
spark.driver.memory                 1g
spark.sql.shuffle.partitions        4
```

### 8.1 trace 粒度汇总（`dws_trace_1d.py`）

运行时机：D+1 日凌晨处理 D 日。**跨天 trace 的处理**：读取 D 与 D+1 两个分区，只保留开始时间落在 D 日的 trace，避免一个任务被切成两半。

```sql
INSERT OVERWRITE paimon.ops.dws_llm_trace_1d PARTITION (dt = '${dt}')
SELECT
  trace_id,
  MIN_BY(feature, ts_start)                                        AS feature,
  MIN_BY(prompt_version, ts_start)                                 AS prompt_version,
  COUNT(*)                                                         AS span_cnt,
  SUM(CASE WHEN span_type = 'llm'  THEN 1 ELSE 0 END)              AS llm_call_cnt,
  SUM(CASE WHEN span_type = 'tool' THEN 1 ELSE 0 END)              AS tool_call_cnt,
  SUM(CASE WHEN attempt > 1 THEN 1 ELSE 0 END)                     AS retry_span_cnt,
  SUM(cost_usd)                                                    AS trace_cost_usd,
  SUM(CASE WHEN attempt > 1 THEN cost_usd ELSE 0 END)              AS retry_cost_usd,
  SUM(input_tokens + output_tokens)                                AS trace_tokens,
  (MAX(UNIX_MILLIS(ts_end)) - MIN(UNIX_MILLIS(ts_start))) / 1000.0 AS trace_duration_s,
  MAX(CASE WHEN status <> 'success' THEN 1 ELSE 0 END)             AS has_failure,
  MIN(CASE WHEN status <> 'success' THEN step_no END)              AS first_fail_step,
  -- 修正：取第一次失败那一步的类型，而不是字母序最小的类型
  MIN_BY(CASE WHEN status <> 'success' THEN span_type END,
         CASE WHEN status <> 'success' THEN step_no END)           AS first_fail_type
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY trace_id ORDER BY ts_start, span_id) AS step_no,
         MIN(ts_start) OVER (PARTITION BY trace_id)                           AS trace_start
  FROM paimon.ops.dwd_llm_call
  WHERE dt IN ('${dt}', '${dt_next}')
) t
WHERE DATE_FORMAT(trace_start, 'yyyy-MM-dd') = '${dt}'
GROUP BY trace_id;
```

> `MIN_BY` 会忽略排序键为 NULL 的行，所以对成功行得到 NULL 不影响结果。
> "任务成功"的口径：最终是否成功应看最后一次尝试。如果 Agent 重试后成功，`has_failure=1` 会把它算作失败。建议再加一列 `final_success`（trace 中最后一个 llm span 的 status），并在 README 中说明两个口径的区别。

### 8.2 天级精确汇总（`dws_feature_1d.py`）

按 `dt × feature × model × prompt_version` 汇总：调用数、成本、token、失败率、重试成本占比、缓存命中率（`SUM(cached_tokens)/SUM(input_tokens)`），以及 `percentile_approx(latency_ms, array(0.5, 0.95, 0.99))`。

### 8.3 重算历史成本（`reprice.py`）

价格维表变更后，用 `dim_model_price_hist` 按 `ts_end` 落在 `[effective_from, effective_to)` 区间关联，重写 `dwd_llm_call` 的 `cost_usd`，再重跑 8.1、8.2。这是"成本不写进事件"这一设计的直接证明。

### 8.4 导出 ADS（`export_ads.py`）

Spark JDBC 以 overwrite（按 dt 先删后写）方式写入 Postgres 的 `ads_*` 表，保证重跑幂等。

---

## 9. 离线评测链路

```
data/eval/testset_v1.jsonl（question, db_id, gold_sql）
        │
        ▼
run_eval.py --prompt v2 --model deepseek-chat
  ├─ 每条 case 走真实 Agent（埋点照常产出，env=dev，feature=text_to_sql_eval）
  ├─ 规则指标：SQL 可执行、执行结果与 gold 一致、步数、是否超步
  └─ judge.py：对最终自然语言回答打 1 到 5 分
        │
        ▼
Postgres: eval_run / eval_case
        │
        ▼
regress.py --run <new> --base <old>  →  eval_regress + Markdown diff 报告
```

表结构：

```sql
CREATE TABLE eval_run (
  run_id TEXT PRIMARY KEY, prompt_version TEXT, model TEXT, dataset_version TEXT,
  started_at TIMESTAMPTZ, case_cnt INT, exec_ok_rate NUMERIC, result_match_rate NUMERIC,
  avg_judge_score NUMERIC, avg_cost_usd NUMERIC, avg_steps NUMERIC
);
CREATE TABLE eval_case (
  run_id TEXT, case_id TEXT, trace_id TEXT, pred_sql TEXT, exec_ok BOOLEAN,
  result_match BOOLEAN, judge_score INT, steps INT, cost_usd NUMERIC, passed BOOLEAN,
  PRIMARY KEY (run_id, case_id)
);
CREATE TABLE eval_regress (
  run_id TEXT, base_run_id TEXT, case_id TEXT,
  direction TEXT CHECK (direction IN ('fixed', 'broken', 'same_pass', 'same_fail')),
  PRIMARY KEY (run_id, base_run_id, case_id)
);
CREATE TABLE judge_calibration (
  case_id TEXT, judge_score INT, human_score INT, judge_model TEXT, labeled_at TIMESTAMPTZ
);
```

要点：
- `eval_case.trace_id` 记录该 case 对应的 trace，可以下钻到每一步的成本和失败。
- **judge 校准**：人工标注 50 条，计算 judge 与人工的一致率（例如分数差 ≤1 的比例，或 Cohen's kappa）；低于 0.7 时报告中注明"judge 结论不可采信"，只看规则指标。
- **与线上关联**：`ads_eval_vs_online` = `eval_run` JOIN `dws_llm_feature_1d` ON `prompt_version`，对比离线通过率与线上失败率、单次成本。
- 评测成本控制：先用 20 条跑通，再跑全量。

---

## 10. 看板（Streamlit）

页面：
1. **成本**：日成本趋势（按 feature 堆叠）、feature 成本占比、重试成本占比、缓存命中率
2. **任务**：任务成功率趋势、平均步数、单任务成本分布、首次失败步位分布、首次失败类型
3. **延迟**：按 feature / model 的 p50 / p95 / p99
4. **告警**：最近告警列表（读 `alert_event`）
5. **评测**：各 prompt 版本指标对比、case 级 diff、离线与线上对比

数据只读 Postgres（ADS + 告警 + 评测）。

---

## 11. 实施计划与验收

按每天 4 到 6 小时估算，共约 3 周。**每个 Phase 验收通过再进入下一个。**

### Phase 0：环境（2 天）
- [ ] 安装 OrbStack，设置内存上限 6GB；`uv init`，建立目录结构
- [ ] 核对并写死各 jar 版本；编写 Flink / Spark Dockerfile
- [ ] `docker-compose.yml`：kafka、flink-jobmanager、flink-taskmanager、postgres（常驻），spark（`profiles: [batch]`）
- [ ] Makefile：`up`、`down`、`status`、`logs S=`、`topics`、`flink-sql`、`spark-sql`
- **验收**：Flink SQL Client 中能建 Paimon 表、写入、读出；Spark 能读到同一张表

### Phase 1：埋点 + 模拟器（3 天）
- [ ] `instrumentation/`：context、schema、producer、client、tools
- [ ] 单元测试：异常时仍发事件、重试产生多个 span、parent 关系正确、Kafka 不可用时不抛异常
- [ ] `simulator/`：feature 画像、backfill / live 模式、各异常开关
- [ ] （可选）读取 BurstGPT 统计 token 分布，回填到画像参数
- **验收**：`make sim-live` 后用 `kafka-console-consumer` 看到结构正确的事件；同一 trace 的 span 能按 parent 串成树

### Phase 2：实时链路（5 天，技术重点）
- [ ] 价格维表与初始价格数据
- [ ] Kafka 源表、清洗视图、ODS、DWD
- [ ] 分钟级聚合写 Paimon
- [ ] 学习重点：watermark 与乱序、checkpoint、主键表与去重、bucket 与小文件
- **验收**：模拟器运行时 DWD 持续增长；`--dup-rate` 下 DWD 行数 = 去重后的 span 数；`--late-sec 30` 的事件仍计入正确窗口

### Phase 3：告警（2 到 3 天）
- [ ] Postgres `alert_event`；Flink JDBC sink
- [ ] 四条规则；必要时启用 Python 告警退路
- **验收**：分别注入 loop、token-spike、fail-rate、retry-storm，2 分钟内出现对应告警记录

### Phase 4：Agent + 批处理 + 评测（4 天）
- [ ] 小型 text-to-SQL Agent，两个 prompt 版本（v2 针对 v1 的失败 case 改进）
- [ ] 准备评测集（Spider/BIRD 子集 100 到 200 条）与 50 条人工标注
- [ ] `dws_trace_1d`、`dws_feature_1d`、`reprice`、`export_ads`
- [ ] `run_eval`、`judge`、`regress`
- **验收**：输出 v1 vs v2 的 diff 报告（fixed / broken 各多少、集中在哪类问题）；同一天的批处理重跑两次，结果完全一致

### Phase 5：看板 + 文档 + 实测（2 天）
- [ ] Streamlit 五个页面
- [ ] README：架构图、设计取舍（流批分工、成本不入事件、价格维表拆分、p95 放在批处理）、运行方式、**实测数字**
- [ ] `make setup` 从零一键跑通
- **验收**：在一个干净的目录里 clone 后执行 `make setup` 能跑起来

---

## 12. 验证清单（交付前逐条执行并记录结果）

| # | 验证项 | 方法 |
|---|---|---|
| 1 | 重复 span_id 不产生重复行 | `sim --dup-rate 0.05`，比较 DWD 行数与去重后的 span 数 |
| 2 | 延迟 30 秒的事件归入正确窗口 | `sim --late-rate 0.05 --late-sec 30`，与批处理结果对比分钟级计数 |
| 3 | 改价后重算历史，成本相应变化 | 修改 `dim_model_price_hist`，执行 `reprice` 后对比 ADS |
| 4 | 死循环触发成本告警 | `sim --inject loop` |
| 5 | Flink 重启后不重不漏 | 运行中 `docker restart flink-taskmanager`，对比 Kafka 发送总数与 DWD 行数 |
| 6 | 批处理幂等 | 同一 dt 重跑两次，两次结果做 `EXCEPT` 为空 |
| 7 | 埋点不影响业务 | 停掉 Kafka 运行 Agent，Agent 正常返回 |
| 8 | **记录真实数字** | 事件总数、Flink 稳定吞吐（条/秒）、端到端延迟、批作业耗时、峰值内存 |

第 8 条必须真实测量，写进 README 和简历，不估算。可以在 `make bench` 中固化测量方法：模拟器以固定 QPS 发送 N 条，记录 Flink Web UI 的吞吐和 DWD 行数增长曲线。

---

## 13. Makefile 命令约定

```
make up              启动常驻服务（kafka / flink / postgres）
make down            停止所有服务（数据保留）
make clean           停止并删除数据卷
make topics          创建 llm.calls.raw / llm.calls.payload
make flink-submit    提交 Flink SQL 作业
make sim-backfill    DAYS=30 QPS=50 回灌历史
make sim-live        QPS=20 INJECT=loop 实时造数
make agent Q="..."   运行一次 Agent
make batch DT=2026-11-18   跑当天批处理并导出 ADS
make reprice         价格变更后重算
make eval V=v2       跑评测；make regress RUN=... BASE=...
make dashboard       启动 Streamlit
make bench           吞吐与延迟测量
make test            单元测试
```

---

## 14. 给 Claude Code 的工作约定

在本目录协助开发时遵守：

1. **资源**：所有容器必须设置 `mem_limit`；Spark 只用 `docker compose run --rm`；不在本地启动大模型。
2. **端口**：只绑定 `127.0.0.1`。宿主机访问 Kafka 用 EXTERNAL listener `localhost:9094`。
3. **秘密**：API Key 与密码只放 `.env`（已在 `.gitignore` 中），SQL 和代码里只用环境变量 / 占位符。
4. **埋点代码**的任何修改都必须保持"发送失败不抛异常"，并补充单元测试。
5. **SQL 变更**：修改 Flink SQL 后需要停掉旧作业再提交；修改 DWD 表结构需要评估是否要重建表并回灌。
6. **版本**：jar 和镜像版本写死，不使用 `latest`。
7. **文档**：设计取舍的变化同步更新本文件与 README；文档使用中文。
8. 每个 Phase 完成后，按第 11 节的验收项实际执行并记录结果，不凭推断宣布完成。

---

## 15. 后续扩展（按价值排序）

1. 用 LangGraph 重写 Agent，通过 callback 采集事件，与自写封装的事件对比
2. 实时 p95：Flink Java UDAF（t-digest）
3. 数据质量监控：字段缺失率、trace 断链率（有 parent 但找不到父 span）
4. 成本优化建议：识别重复 `prompt_hash`（可缓存）、短输入短输出的简单请求（可降级小模型）
5. 用 Trino 直接查询 Paimon，替代部分 ADS
6. 迁移到 OpenTelemetry SDK + Collector

## 附：参考

- `DESIGN.md`：原始设计（本文为其修订与落地版本）
- [git-raka/iceberg-ai-lakehouse](https://github.com/git-raka/iceberg-ai-lakehouse)：借鉴其 compose 资源限制与健康检查、Makefile 组织、`ai/llm.py` 的调用网关、`sources/iot.py` 的模拟器写法、`infra/flink/submit.sh` 的提交方式、`ai/evaluate.py` 的评测方式。不照搬其 Iceberg / Trino / Ollama 等重型组件。
- Apache Paimon 官方文档：Flink / Spark 集成、主键表、merge-engine、lookup join
- BurstGPT、Azure LLM Inference Trace：公开 LLM 调用 trace，用于校准模拟器
- Spider / BIRD：text-to-SQL 评测集
