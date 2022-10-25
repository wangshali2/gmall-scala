package com.atguigu.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.guigu.GmallConstant;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author wsl
 * @version 2021-01-05
 * //GmallLoggerApplication只能扫描同级别或同级别子类的注解，所以Controller位置要注意
 */
@Slf4j

@RestController
public class LoggerController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    //http://localhost:8081/test
    @RequestMapping("test")
    public String test() {
        System.out.println("成功...");

        return "success 1 ";
    }

    //http://localhost:8081/show?name=wsl&age=19
    @RequestMapping("show")
    // @ResponseBody
    public String show(@RequestParam("name") String userName, @RequestParam("age") int age) {
        System.out.println(userName + ": " + age);
        return "success 2";
    }

    //http://localhost:8081/log?logString=wsl
    @RequestMapping("log")
    //@ResponseBody
    public String getLog(@RequestParam("logString") String logStr) {


        JSONObject jsonObject = JSON.parseObject(logStr);
        jsonObject.put("ts", System.currentTimeMillis());


        String logger = jsonObject.toString();

      //  System.out.println(logger);

        log.info(logger);

        //todo kafka加了认证，没有调通，因为是配置参数不对

        if ("startup".equals(jsonObject.getString("type"))) {
            kafkaTemplate.send(GmallConstant.GMALL_STARTUP,logger);

        } else {
            kafkaTemplate.send(GmallConstant.GMALL_EVENT,logger);

        }

        return "success log！！！！";
    }


}
