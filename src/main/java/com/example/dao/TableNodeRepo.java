package com.example.dao;

import com.example.model.TableNodeEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface TableNodeRepo extends JpaRepository<TableNodeEntity, Long> {
    Optional<TableNodeEntity> findByName(String name);

    @Query("select t from TableNodeEntity t " +
            "where (:kw is null or :kw = '' or t.name like concat('%', :kw, '%') or t.displayName like concat('%', :kw, '%')) " +
            "order by t.name asc")
    List<TableNodeEntity> searchByKeyword(@Param("kw") String kw);
}
