package com.atguigu.gmallpublicer;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
//1重保证 xml和mapper包下接口对应
@MapperScan(basePackages = "com.atguigu.gmallpublicer.mapper")
public class GmallPublicerApplication {

    public static void main(String[] args) {
        SpringApplication.run(GmallPublicerApplication.class, args);
    }

}
