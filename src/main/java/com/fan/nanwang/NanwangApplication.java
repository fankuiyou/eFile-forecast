package com.fan.nanwang;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class NanwangApplication {

    public static void main(String[] args) {
        SpringApplication.run(NanwangApplication.class, args);
    }

}
