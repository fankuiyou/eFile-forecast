package com.fan.nanwang.entity;

import lombok.Data;

import javax.persistence.*;

@Data
@Entity
@Table(name = "power_plant")
public class PowerPlant {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "yc_id")
    private Long ycId;

    private String region;

    @Column(name = "name_abbreviation")
    private String nameAbbreviation;

    private String name;
}
