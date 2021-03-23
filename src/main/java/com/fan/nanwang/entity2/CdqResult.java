package com.fan.nanwang.entity2;

import lombok.Data;

import javax.persistence.*;

@Data
@Entity
@Table(name = "rdsf_cdq_result")
public class CdqResult {

    @Id
    @GeneratedValue(strategy= GenerationType.TABLE,generator="seq_cdq_result_id")
    @TableGenerator(
            name = "seq_cdq_result_id",
            table="sequence",
            pkColumnName="SEQ_NAME",     //指定主键的名字
            pkColumnValue="cdq_result_id",      //指定下次插入主键时使用默认的值
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
    private String dataType;

    @Column(name = "DATA_DATE")
    private String dataDate;

    @Column(name = "DATA_TIME")
    private String dataTime;

    @Column(name = "MODEL_MARK")
    private String modelMark;

    @Column(name = "P1")
    private String p1;

    @Column(name = "P2")
    private String p2;

    @Column(name = "P3")
    private String p3;

    @Column(name = "P4")
    private String p4;

    @Column(name = "P5")
    private String p5;

    @Column(name = "P6")
    private String p6;

    @Column(name = "P7")
    private String p7;

    @Column(name = "P8")
    private String p8;

    @Column(name = "P9")
    private String p9;

    @Column(name = "P10")
    private String p10;

    @Column(name = "P11")
    private String p11;

    @Column(name = "P12")
    private String p12;

    @Column(name = "P13")
    private String p13;

    @Column(name = "P14")
    private String p14;

    @Column(name = "P15")
    private String p15;

    @Column(name = "P16")
    private String p16;

    @Column(name = "OP_DATE")
    private String opDate;
}
