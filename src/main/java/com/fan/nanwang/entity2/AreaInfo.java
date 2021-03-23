package com.fan.nanwang.entity2;

import lombok.Data;
import javax.persistence.*;

@Data
@Entity
@Table(name = "rbase_area_info")
public class AreaInfo {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "AREA_ID")
    private String areaId;

    @Column(name = "NET_ID")
    private String netId;

    @Column(name = "P_ID")
    private String pId;

    @Column(name = "F_ID")
    private String fId;

    @Column(name = "NAME")
    private String name;

    @Column(name = "CAP")
    private Double cap;

    @Column(name = "DATA_TYPE")
    private Integer dataType;

    @Column(name = "RANK_NO")
    private Integer rankNo;

    @Column(name = "OBJ_TYPE")
    private String objType;

    @Column(name = "OTHER_NAME")
    private String otherName;

    @Column(name = "FLAG")
    private Integer flag;
}
