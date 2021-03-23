package com.fan.nanwang.repository;

import com.fan.nanwang.entity.Power;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface PowerRepository extends JpaRepository<Power, Long> {

    @Query(value = "select max(date) from power where region = :region", nativeQuery = true)
    String findMaxDateByRegion(@Param("region") String region);
}
