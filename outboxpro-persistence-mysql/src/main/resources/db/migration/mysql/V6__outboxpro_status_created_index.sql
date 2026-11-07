-- V1.4：最老待投递消息年龄聚合（MIN(created_time) by status）。
-- 使用独立迁移而不是修改 V1，确保已执行过历史迁移的 Flyway/Liquibase 用户能平滑升级。
ALTER TABLE outboxpro_outbox
    ADD KEY idx_outbox_status_created (status, created_time);
