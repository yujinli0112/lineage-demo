package com.example.model;

import javax.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@Entity
@Table(name = "ln_run")
@Getter @Setter @NoArgsConstructor
public class LineageRunEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable=false)
    private Instant createdAt = Instant.now();

    /** 显式指定 LONGTEXT，避免方言/历史表结构导致的 TEXT/VARCHAR */
    @Lob
    @Column(name = "sql_text", columnDefinition = "LONGTEXT", nullable = false)
    private String sqlText;


    /** SHA-256 十六进制长度固定 64 */
    @Column(length = 64)
    private String sqlHash;

    private Integer stmtCount;
}
