package com.example.model;

import javax.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@Entity
@Table(name = "ln_edge",
        indexes = {
                @Index(name="idx_edge_src", columnList="source_id"),
                @Index(name="idx_edge_tgt", columnList="target_id"),
                @Index(name="idx_edge_run", columnList="run_id")
        })
@Getter @Setter @NoArgsConstructor
public class EdgeEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "source_id")
    private TableNodeEntity source;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "target_id")
    private TableNodeEntity target;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "run_id")
    private LineageRunEntity run;

    @Column
    private Integer stmtIndex;

    @Column(length = 64)
    private String stepLabel;

    @Column(nullable = false)
    private Instant createdAt = Instant.now();
}
