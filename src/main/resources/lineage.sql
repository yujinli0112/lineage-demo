CREATE TABLE `ln_edge` (
                           `id` bigint NOT NULL AUTO_INCREMENT,
                           `source_id` bigint NOT NULL,
                           `target_id` bigint NOT NULL,
                           `run_id` bigint NOT NULL,
                           `stmt_index` int DEFAULT NULL,
                           `step_label` varchar(64) DEFAULT NULL,
                           `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                           PRIMARY KEY (`id`),
                           KEY `idx_edge_src` (`source_id`),
                           KEY `idx_edge_tgt` (`target_id`),
                           KEY `idx_edge_run` (`run_id`),
                           CONSTRAINT `fk_edge_run` FOREIGN KEY (`run_id`) REFERENCES `ln_run` (`id`),
                           CONSTRAINT `fk_edge_src` FOREIGN KEY (`source_id`) REFERENCES `ln_table` (`id`),
                           CONSTRAINT `fk_edge_tgt` FOREIGN KEY (`target_id`) REFERENCES `ln_table` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=342 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE `ln_run` (
                          `id` bigint NOT NULL AUTO_INCREMENT,
                          `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                          `sql_text` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
                          `sql_hash` varchar(128) DEFAULT NULL,
                          `stmt_count` int DEFAULT NULL,
                          PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=24 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


CREATE TABLE `ln_table` (
                            `id` bigint NOT NULL AUTO_INCREMENT,
                            `name` varchar(256) NOT NULL,
                            `type` varchar(32) NOT NULL DEFAULT 'table',
                            `display_name` varchar(256) DEFAULT NULL,
                            `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                            PRIMARY KEY (`id`),
                            UNIQUE KEY `uk_ln_table_name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=331 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;