package com.fan.nanwang.entity;

import lombok.Data;

import javax.persistence.*;

@Data
@Entity
@Table(name = "power")
public class Power {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String region;

    @Column(name = "name_abbreviation")
    private String nameAbbreviation;

    private String name;

    private String date;

    private String power;
}
