package com.fan.nanwang.repository2;

import com.fan.nanwang.entity2.CdqResult;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Map;

public interface CdqResultRepository extends JpaRepository<CdqResult, Long> {

    List<CdqResult> findByObjIdAndDataTime(String objId, String dateTime);

    @Query(value = "select DATA_TIME from rdsf_cdq_result where DATA_TYPE = 'UP' " +
            "and DATA_TIME > if((select max(DATA_TIME) from rdsf_cdq_result where DATA_TYPE = 'UP' and OBJ_TYPE = 'NET') is null, " +
            "'1990-01-01 00:00', (select max(DATA_TIME) from rdsf_cdq_result where DATA_TYPE = 'UP' and OBJ_TYPE = 'NET')) " +
            "group by DATA_TIME", nativeQuery = true)
    List<String> findDataTimeListByFarm();

    @Query(value = "SELECT 'UP' dataType, left(DATA_TIME, 10) dataDate, DATA_TIME dataTime, sum(p1) p1, sum(p2) p2, " +
            "sum(p3) p3, sum(p4) p4, sum(p5) p5, sum(p6) p6, sum(p7) p7, sum(p8) p8, sum(p9) p9, sum(p10) p10, sum(p11) p11, " +
            "sum(p12) p12, sum(p13) p13, sum(p14) p14, sum(p15) p15, sum(p16) p16, MODEL_MARK modelMark FROM rdsf_cdq_result " +
            "WHERE DATA_TYPE = 'UP' AND DATA_TIME = :dataTime AND OBJ_ID IN :objIdList", nativeQuery = true)
    Map getSumResultByDataTimeAndObjIdList(@Param("dataTime") String dataTime, @Param("objIdList") List<String> objIdList);

    @Query(value = "select if(max(DATA_TIME) is null, '1990-01-01 00:00', max(DATA_TIME)) from rdsf_cdq_result where OBJ_TYPE = 'NET' and OBJ_ID = :objId", nativeQuery = true)
    String findMaxDateTimeByRegionObjId(@Param("objId") String objId);
}
