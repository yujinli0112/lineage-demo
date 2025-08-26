package com.example.model;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@Entity
@Table(name = "ln_table", uniqueConstraints = {
        @UniqueConstraint(name = "uk_ln_table_name", columnNames = {"name"})
})
@Getter @Setter @NoArgsConstructor
public class TableNodeEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 256)
    private String name;

    @Column(nullable = false, length = 32)
    private String type = "table"; // table / view / process(不落库)

    @Column(length = 256)
    private String displayName;

    @Column(nullable = false)
    private Instant createdAt = Instant.now();

    @Column(nullable = false)
    private Instant updatedAt = Instant.now();
}
