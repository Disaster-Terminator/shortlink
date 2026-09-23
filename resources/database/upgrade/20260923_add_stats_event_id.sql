-- Add a durable inbox key so RocketMQ redelivery cannot double-count statistics.
ALTER TABLE `t_link_access_logs`
    ADD COLUMN `event_id` varchar(64) DEFAULT NULL COMMENT '统计事件唯一标识' AFTER `id`,
    ADD UNIQUE KEY `idx_unique_event_id` (`event_id`) USING BTREE;
