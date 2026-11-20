-- Paimon catalog 与业务库。
-- Flink SQL 客户端的 catalog 只存在于当前会话，所以每个作业文件、每次交互会话都要先执行本文件
-- （`make flink-sql` 用 -i 参数自动加载）。warehouse 是 Flink 与 Spark 共享的卷。

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'file:///data/paimon'
);

CREATE DATABASE IF NOT EXISTS paimon.ops;

SET 'sql-client.execution.result-mode' = 'tableau';
