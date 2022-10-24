package com.atguigu.gmallpublicer.service;

/**
 * @author wsl
 * @version 2021-01-08
 */
import java.io.IOException;
import java.util.Map;

public interface PublisherService {

    public Integer getDauTotal(String date);
    //list --> Map
    public Map getDauTotalHourMap(String date);


    public Double getOrderAmount(String date);

    public Map getOrderAmountHour(String date);


}
