package com.fan.nanwang.entity;

import lombok.Data;

import javax.persistence.*;

/**
 * 短期预测
 */
@Data
public class ForecastPower {

    private Long id;

    private String region;

    private String nameAbbreviation;

    private Integer dateSort;

    private String date;

    private String forecastDate;

    private String forecastValue;

    private String volume;

    private String runNum;
}
