package com.fan.nanwang.entity2;

import lombok.Data;

import javax.persistence.*;

@Data
@Entity
@Table(name = "rbase_net_info")
public class NetInfo {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "NET_ID")
    private String netId;

    @Column(name = "NET_CODE")
    private String netCode;

    @Column(name = "NET_SHORT_NAME")
    private String netShortName;

    @Column(name = "NET_FULL_NAME")
    private String netFullName;

    @Column(name = "NET_TYPE")
    private String netType;

    @Column(name = "NET_TYPE_NAME")
    private String netTypeName;

    @Column(name = "PARENT_NET")
    private String parentNet;

    @Column(name = "PARENT_NET_NAME")
    private String parentNetName;

    @Column(name = "TOP_NET_FLG")
    private String topNetFlg;

    @Column(name = "DISPLAY_COLOR")
    private String displayColor;

    @Column(name = "DISPLAY_ORDER")
    private String displayOrder;

    @Column(name = "NET_STATUS")
    private String netStatus;

    @Column(name = "CREATE_TIME")
    private String createTime;

    @Column(name = "CAP")
    private String cap;
}
