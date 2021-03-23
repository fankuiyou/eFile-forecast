package com.fan.nanwang.repository2;

import com.fan.nanwang.entity2.FarmInfo;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface FarmInfoRepository extends JpaRepository<FarmInfo, Long> {

    List<FarmInfo> findByAreaId(String areaId);

    FarmInfo findByYcId(Long ycId);
}
