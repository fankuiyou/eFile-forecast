package com.fan.nanwang.entity2;

import lombok.Data;

import javax.persistence.*;

@Data
@Entity
@Table(name = "rdsf_dq_result")
public class DqResult {

    @Id
    @GeneratedValue(strategy= GenerationType.TABLE,generator="seq_dq_result_id")
    @TableGenerator(
            name = "seq_dq_result_id",
            table="sequence",
            pkColumnName="SEQ_NAME",     //指定主键的名字
            pkColumnValue="dq_result_id",      //指定下次插入主键时使用默认的值
            valueColumnName="SEQ_COUNT",    //该主键当前所生成的值，它的值将会随着每次创建累加
            initialValue = 1,            //初始化值
            allocationSize=1             //累加值
    )
    private Long id;

    @Column(name = "OBJ_ID")
    private String objId;

    @Column(name = "OBJ_TYPE")
    private String objType;

    @Column(name = "DATA_TYPE")
    private Integer dataType;

    @Column(name = "DATA_FLG")
    private String dataFlg;

    @Column(name = "DATA_DATE")
    private String dataDate;

    @Column(name = "DATA_TIME")
    private String dataTime;

    @Column(name = "PRE_DATE")
    private String preDate;

    @Column(name = "VAL0015")
    private String val0015;

    @Column(name = "VAL0030")
    private String val0030;

    @Column(name = "VAL0045")
    private String val0045;

    @Column(name = "VAL0100")
    private String val0100;

    @Column(name = "VAL0115")
    private String val0115;

    @Column(name = "VAL0130")
    private String val0130;

    @Column(name = "VAL0145")
    private String val0145;

    @Column(name = "VAL0200")
    private String val0200;

    @Column(name = "VAL0215")
    private String val0215;

    @Column(name = "VAL0230")
    private String val0230;

    @Column(name = "VAL0245")
    private String val0245;

    @Column(name = "VAL0300")
    private String val0300;

    @Column(name = "VAL0315")
    private String val0315;

    @Column(name = "VAL0330")
    private String val0330;

    @Column(name = "VAL0345")
    private String val0345;

    @Column(name = "VAL0400")
    private String val0400;

    @Column(name = "VAL0415")
    private String val0415;

    @Column(name = "VAL0430")
    private String val0430;

    @Column(name = "VAL0445")
    private String val0445;

    @Column(name = "VAL0500")
    private String val0500;

    @Column(name = "VAL0515")
    private String val0515;

    @Column(name = "VAL0530")
    private String val0530;

    @Column(name = "VAL0545")
    private String val0545;

    @Column(name = "VAL0600")
    private String val0600;

    @Column(name = "VAL0615")
    private String val0615;

    @Column(name = "VAL0630")
    private String val0630;

    @Column(name = "VAL0645")
    private String val0645;

    @Column(name = "VAL0700")
    private String val0700;

    @Column(name = "VAL0715")
    private String val0715;

    @Column(name = "VAL0730")
    private String val0730;

    @Column(name = "VAL0745")
    private String val0745;

    @Column(name = "VAL0800")
    private String val0800;

    @Column(name = "VAL0815")
    private String val0815;

    @Column(name = "VAL0830")
    private String val0830;

    @Column(name = "VAL0845")
    private String val0845;

    @Column(name = "VAL0900")
    private String val0900;

    @Column(name = "VAL0915")
    private String val0915;

    @Column(name = "VAL0930")
    private String val0930;

    @Column(name = "VAL0945")
    private String val0945;

    @Column(name = "VAL1000")
    private String val1000;

    @Column(name = "VAL1015")
    private String val1015;

    @Column(name = "VAL1030")
    private String val1030;

    @Column(name = "VAL1045")
    private String val1045;

    @Column(name = "VAL1100")
    private String val1100;

    @Column(name = "VAL1115")
    private String val1115;

    @Column(name = "VAL1130")
    private String val1130;

    @Column(name = "VAL1145")
    private String val1145;

    @Column(name = "VAL1200")
    private String val1200;

    @Column(name = "VAL1215")
    private String val1215;

    @Column(name = "VAL1230")
    private String val1230;

    @Column(name = "VAL1245")
    private String val1245;

    @Column(name = "VAL1300")
    private String val1300;

    @Column(name = "VAL1315")
    private String val1315;

    @Column(name = "VAL1330")
    private String val1330;

    @Column(name = "VAL1345")
    private String val1345;

    @Column(name = "VAL1400")
    private String val1400;

    @Column(name = "VAL1415")
    private String val1415;

    @Column(name = "VAL1430")
    private String val1430;

    @Column(name = "VAL1445")
    private String val1445;

    @Column(name = "VAL1500")
    private String val1500;

    @Column(name = "VAL1515")
    private String val1515;

    @Column(name = "VAL1530")
    private String val1530;

    @Column(name = "VAL1545")
    private String val1545;

    @Column(name = "VAL1600")
    private String val1600;

    @Column(name = "VAL1615")
    private String val1615;

    @Column(name = "VAL1630")
    private String val1630;

    @Column(name = "VAL1645")
    private String val1645;

    @Column(name = "VAL1700")
    private String val1700;

    @Column(name = "VAL1715")
    private String val1715;

    @Column(name = "VAL1730")
    private String val1730;

    @Column(name = "VAL1745")
    private String val1745;

    @Column(name = "VAL1800")
    private String val1800;

    @Column(name = "VAL1815")
    private String val1815;

    @Column(name = "VAL1830")
    private String val1830;

    @Column(name = "VAL1845")
    private String val1845;

    @Column(name = "VAL1900")
    private String val1900;

    @Column(name = "VAL1915")
    private String val1915;

    @Column(name = "VAL1930")
    private String val1930;

    @Column(name = "VAL1945")
    private String val1945;

    @Column(name = "VAL2000")
    private String val2000;

    @Column(name = "VAL2015")
    private String val2015;

    @Column(name = "VAL2030")
    private String val2030;

    @Column(name = "VAL2045")
    private String val2045;

    @Column(name = "VAL2100")
    private String val2100;

    @Column(name = "VAL2115")
    private String val2115;

    @Column(name = "VAL2130")
    private String val2130;

    @Column(name = "VAL2145")
    private String val2145;

    @Column(name = "VAL2200")
    private String val2200;

    @Column(name = "VAL2215")
    private String val2215;

    @Column(name = "VAL2230")
    private String val2230;

    @Column(name = "VAL2245")
    private String val2245;

    @Column(name = "VAL2300")
    private String val2300;

    @Column(name = "VAL2315")
    private String val2315;

    @Column(name = "VAL2330")
    private String val2330;

    @Column(name = "VAL2345")
    private String val2345;

    @Column(name = "VAL2400")
    private String val2400;
}
