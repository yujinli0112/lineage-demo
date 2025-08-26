package com.example.dao;

import com.example.model.LineageRunEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface LineageRunRepo extends JpaRepository<LineageRunEntity, Long> { }
