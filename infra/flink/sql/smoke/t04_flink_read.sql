-- T04 冒烟测试第 3 步（Flink）：读出 Spark 写入后的结果，验证反方向也能互通。

SET 'execution.runtime-mode' = 'batch';

SELECT CONCAT('FLINK_RESULT ', CAST(COUNT(*) AS STRING), ' ', CAST(SUM(v) AS STRING)) AS r
FROM paimon.ops.smoke_pk;
