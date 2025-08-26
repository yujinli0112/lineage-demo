package com.example.dao;

import com.example.model.EdgeEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface EdgeRepo extends JpaRepository<EdgeEntity, Long> {
    @Query("select e from EdgeEntity e join fetch e.source s join fetch e.target t where s.name = :name or t.name = :name")
    List<EdgeEntity> findEdgesTouching(String name);

    @Query("select e from EdgeEntity e join fetch e.source s join fetch e.target t")
    List<EdgeEntity> findAllWithNodes();
}
