package com.atguigu.gmallpublicer.mapper;


import java.util.List;
import java.util.Map;

public interface DauMapper {

    //查询日活总数的方法
    public Integer selectDauTotal(String date);
    //查询日活分时的方法
    public List<Map> selectDauTotalHourMap(String date);

}

