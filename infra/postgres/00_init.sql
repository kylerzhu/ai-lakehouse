-- PostgreSQL 初始化：只在数据卷首次创建时执行。
-- 告警表（T13）、ADS 表（T18）、评测表（T23）在对应任务中追加到本目录。

CREATE SCHEMA IF NOT EXISTS ops;
COMMENT ON SCHEMA ops IS '告警、ADS 看板数据与评测结果';
