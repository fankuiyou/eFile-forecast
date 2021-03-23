package com.fan.nanwang.entity2;

import lombok.Data;

import javax.persistence.*;

@Data
@Entity
@Table(name = "rbase_farm_info")
public class FarmInfo {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private String id;

    @Column(name = "FARM_ID")
    private String farmId;

    @Column(name = "FARM_NAME")
    private String farmName;

    @Column(name = "OTHER_NAME")
    private String otherName;

    @Column(name = "DATA_TYPE")
    private Integer dataType;

    @Column(name = "NET_ID")
    private String netId;

    @Column(name = "AREA_ID")
    private String areaId;

    @Column(name = "YC_ID")
    private Long ycId;
}
