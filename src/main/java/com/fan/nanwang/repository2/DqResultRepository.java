package com.fan.nanwang.repository2;

import com.fan.nanwang.entity2.DqResult;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Map;

public interface DqResultRepository extends JpaRepository<DqResult, Long> {

    List<DqResult> findByObjIdAndDataDateAndPreDate(String objId, String dateDate, String preDate);


    @Query(value = "SELECT DATA_DATE FROM rdsf_dq_result WHERE DATA_FLG = 'UP' " +
            "AND DATA_DATE > IF (( SELECT max(DATA_DATE) FROM rdsf_dq_result WHERE DATA_FLG = 'UP' AND OBJ_TYPE = 'NET' ) IS NULL, " +
            "'1990-01-01', ( SELECT max(DATA_DATE) FROM rdsf_dq_result WHERE DATA_FLG = 'UP' AND OBJ_TYPE = 'NET' )) " +
            "GROUP BY DATA_DATE", nativeQuery = true)
    List<String> findDataDateListByFarm();

    @Query(value = "SELECT 'UP' dataFlg, DATA_DATE dataDate, DATA_TIME dataTime, PRE_DATE preDATE, sum(VAL0015) val0015, " +
            "sum(VAL0030) val0030, sum(VAL0045) val0045, sum(VAL0100) val0100, sum(VAL0115) val0115, sum(VAL0130) val0130, " +
            "sum(VAL0145) val0145, sum(VAL0200) val0200, sum(VAL0215) val0215, sum(VAL0230) val0230, sum(VAL0245) val0245, " +
            "sum(VAL0300) val0300, sum(VAL0315) val0315, sum(VAL0330) val0330, sum(VAL0345) val0345, sum(VAL0400) val0400, " +
            "sum(VAL0415) val0415, sum(VAL0430) val0430, sum(VAL0445) val0445, sum(VAL0500) val0500, sum(VAL0515) val0515, " +
            "sum(VAL0530) val0530, sum(VAL0545) val0545, sum(VAL0600) val0600, sum(VAL0615) val0615, sum(VAL0630) val0630, " +
            "sum(VAL0645) val0645, sum(VAL0700) val0700, sum(VAL0715) val0715, sum(VAL0730) val0730, sum(VAL0745) val0745, " +
            "sum(VAL0800) val0800, sum(VAL0815) val0815, sum(VAL0830) val0830, sum(VAL0845) val0845, sum(VAL0900) val0900, " +
            "sum(VAL0915) val0915, sum(VAL0930) val0930, sum(VAL0945) val0945, sum(VAL1000) val1000, sum(VAL1015) val1015, " +
            "sum(VAL1030) val1030, sum(VAL1045) val1045, sum(VAL1100) val1100, sum(VAL1115) val1115, sum(VAL1130) val1130, " +
            "sum(VAL1145) val1145, sum(VAL1200) val1200, sum(VAL1215) val1215, sum(VAL1230) val1230, sum(VAL1245) val1245, " +
            "sum(VAL1300) val1300, sum(VAL1315) val1315, sum(VAL1330) val1330, sum(VAL1345) val1345, sum(VAL1400) val1400, " +
            "sum(VAL1415) val1415, sum(VAL1430) val1430, sum(VAL1445) val1445, sum(VAL1500) val1500, sum(VAL1515) val1515, " +
            "sum(VAL1530) val1530, sum(VAL1545) val1545, sum(VAL1600) val1600, sum(VAL1615) val1615, sum(VAL1630) val1630, " +
            "sum(VAL1645) val1645, sum(VAL1700) val1700, sum(VAL1715) val1715, sum(VAL1730) val1730, sum(VAL1745) val1745, " +
            "sum(VAL1800) val1800, sum(VAL1815) val1815, sum(VAL1830) val1830, sum(VAL1845) val1845, sum(VAL1900) val1900, " +
            "sum(VAL1915) val1915, sum(VAL1930) val1930, sum(VAL1945) val1945, sum(VAL2000) val2000, sum(VAL2015) val2015, " +
            "sum(VAL2030) val2030, sum(VAL2045) val2045, sum(VAL2100) val2100, sum(VAL2115) val2115, sum(VAL2130) val2130, " +
            "sum(VAL2145) val2145, sum(VAL2200) val2200, sum(VAL2215) val2215, sum(VAL2230) val2230, sum(VAL2245) val2245, " +
            "sum(VAL2300) val2300, sum(VAL2315) val2315, sum(VAL2330) val2330, sum(VAL2345) val2345, sum(VAL2400) val2400 " +
            "FROM rdsf_dq_result WHERE DATA_flg = 'UP' AND DATA_DATE = :dataDate AND OBJ_ID IN :objIdList GROUP BY PRE_DATE", nativeQuery = true)
    List<Map> getSumResultByDataDateAndObjIdList(@Param("dataDate") String dataDate, @Param("objIdList") List<String> objIdList);
}
