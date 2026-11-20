-- T04 冒烟测试第 1 步（Flink）：建主键表，写入包含重复主键的数据，再读出。
-- 由 scripts/smoke_paimon.sh 调用，可重复运行。

SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';

DROP TABLE IF EXISTS paimon.ops.smoke_pk;

CREATE TABLE paimon.ops.smoke_pk (
  id   INT,
  name STRING,
  v    INT,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'bucket' = '1',
  'merge-engine' = 'deduplicate'
);

-- 第一次提交：3 行
INSERT INTO paimon.ops.smoke_pk VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', 1);

-- 第二次提交：id 2、3 重复，应覆盖为 v = 2；id 4 是新行
INSERT INTO paimon.ops.smoke_pk VALUES (2, 'b', 2), (3, 'c', 2), (4, 'd', 1);

SELECT * FROM paimon.ops.smoke_pk ORDER BY id;
