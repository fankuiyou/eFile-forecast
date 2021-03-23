package com.fan.nanwang.repository2;

import com.fan.nanwang.entity2.AreaInfo;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface AreaInfoRepository extends JpaRepository<AreaInfo, Long> {

    List<AreaInfo> findBypIdIsNotNull();

    List<AreaInfo> findBypIdIsNull();

    List<AreaInfo> findBypId(String pId);

    List<AreaInfo> findBypIdIsNullAndNetId(String netId);
}
