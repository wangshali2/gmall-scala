package com.atguigu.gmallpublicer.service;

import com.atguigu.gmallpublicer.mapper.DauMapper;
import com.atguigu.gmallpublicer.mapper.GmvMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author wsl
 * @version 2021-01-08
 */
@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired  //获取接口调用方法，不用new
    private DauMapper dauMapper;

    @Autowired
    private GmvMapper gmvMapper;

    @Override
    public Integer getDauTotal(String date) {
        return dauMapper.selectDauTotal(date);
    }

    //
    @Override
    public Map getDauTotalHourMap(String date) {

        //1.查询Phoenix获取数据
        List<Map> list = dauMapper.selectDauTotalHourMap(date);

        //2.创建Map用于存放数据
        HashMap<String, Long> result = new HashMap<>();

        //3.遍历list
        for (Map map : list) {
            result.put((String) map.get("LH"), (Long) map.get("CT"));
        }

        //4.返回数据
        return result;
    }

    @Override
    public Double getOrderAmount(String date) {
        return gmvMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {
        //1.查询Phoenix获取数据
        List<Map> list = gmvMapper.selectOrderAmountHourMap(date);

        //2.创建Map用于存放数据
        HashMap<String, Double> result = new HashMap<>();

        //3.遍历list
        for (Map map : list) {
            result.put((String) map.get("CREATE_HOUR"), (Double) map.get("SUM_AMOUNT"));
        }

        //4.返回数据
        return result;

    }


}
//+----------+-----------+
//        | LOGHOUR  LH | COUNT(1)  CT |
//        +----------+-----------+
//        | 08       | 70        |
//        +----------+-----------+
//        | 09       | 90        |
//        +----------+-----------+