package app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import log.LogUploader;
import untils.RanOpt;
import untils.RandomDate;
import untils.RandomNum;
import untils.RandomOptionGroup;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

//JSON字段
public class JsonMocker {

    public static void main(String[] args) {
        genLog();
    }

    private int startupNum = 100000;
    int eventNum = 200000;

    private RandomDate logDateUtil = null;

    //os
    private RanOpt[] osOpts = {new RanOpt("ios", 3), new RanOpt("android", 7)};
    private RandomOptionGroup<String> osOptionGroup = new RandomOptionGroup(osOpts);

    private Date startTime = null;
    private Date endTime = null;

    //area
    private RanOpt[] areaOpts = {
            new RanOpt("beijing", 10),
            new RanOpt("shanghai", 10),
            new RanOpt("guangdong", 20),
            new RanOpt("hebei", 5),
            new RanOpt("heilongjiang", 5),
            new RanOpt("shandong", 5),
            new RanOpt("tianjin", 5),
            new RanOpt("shanxi", 5),
            new RanOpt("shanxi", 5),
            new RanOpt("sichuan", 5)
    };
    private RandomOptionGroup<String> areaOptionGroup = new RandomOptionGroup(areaOpts);

    //appId
    private String appId = "wsl2022";

    //vs
    private RanOpt[] vsOpts = {
            new RanOpt("1.2.0", 50),
            new RanOpt("1.1.2", 15),
            new RanOpt("1.1.3", 30),
            new RanOpt("1.1.1", 5)
    };
    private RandomOptionGroup<String> vsOptionGroup = new RandomOptionGroup(vsOpts);

    //evid
    private RanOpt[] eventOpts = {
            new RanOpt("addFavor", 10),
            new RanOpt("addComment", 30),
            new RanOpt("addCart", 20),
            new RanOpt("clickItem", 4),
            new RanOpt("coupon", 45)
    };
    private RandomOptionGroup<String> eventOptionGroup = new RandomOptionGroup(eventOpts);

    private RanOpt[] channelOpts = {
            new RanOpt("xiaomi", 10),
            new RanOpt("huawei", 20),
            new RanOpt("wandoujia", 30),
            new RanOpt("360", 20),
            new RanOpt("tencent", 20),
            new RanOpt("baidu", 10),
            new RanOpt("website", 10)
    };

    private RandomOptionGroup<String> channelOptionGroup = new RandomOptionGroup(channelOpts);

    private RanOpt[] quitOpts = {new RanOpt(true, 20), new RanOpt(false, 80)};

    private RandomOptionGroup<Boolean> isQuitGroup = new RandomOptionGroup(quitOpts);

    private JsonMocker() {

    }

    public JsonMocker(String startTimeString, String endTimeString, int startupNum, int eventNum) {
        try {
            startTime = new SimpleDateFormat("yyyy-MM-dd").parse(startTimeString);
            endTime = new SimpleDateFormat("yyyy-MM-dd").parse(endTimeString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        logDateUtil = new RandomDate(startTime, endTime, startupNum + eventNum);
    }

    //初始化Event数据
    private String initEventLog(String startLogJson) {
            /*`type` string   COMMENT '日志类型',
             `mid` string COMMENT '设备唯一 表示',
            `uid` string COMMENT '用户标识',
            `os` string COMMENT '操作系统',
            `appid` string COMMENT '应用id',
            `area` string COMMENT '地区' ,
            `evid` string COMMENT '事件id',
            `pgid` string COMMENT '当前页',
            `npgid` string COMMENT '跳转页',
            `itemid` string COMMENT '商品编号',
            `ts` bigint COMMENT '时间',*/

        JSONObject startLog = JSON.parseObject(startLogJson);
        String mid = startLog.getString("mid");
        String uid = startLog.getString("uid");
        String os = startLog.getString("os");
        String appid = this.appId;
        String area = startLog.getString("area");
        String evid = eventOptionGroup.getRandomOpt().getValue();
        int pgid = new Random().nextInt(50) + 1;
        int npgid = new Random().nextInt(50) + 1;
        int itemid = new Random().nextInt(50);
        //  long ts= logDateUtil.getRandomDate().getTime();

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", "event");
        jsonObject.put("mid", mid);
        jsonObject.put("uid", uid);
        jsonObject.put("os", os);
        jsonObject.put("appid", appid);
        jsonObject.put("area", area);
        jsonObject.put("evid", evid);
        jsonObject.put("pgid", pgid);
        jsonObject.put("npgid", npgid);
        jsonObject.put("itemid", itemid);
        return jsonObject.toJSONString();
    }

    //初始化Start数据
    private String initStartsupLog() {
            /*`type` string   COMMENT '日志类型',
             `mid` string COMMENT '设备唯一标识',
             `uid` string COMMENT '用户标识',
             `os` string COMMENT '操作系统', ,
             `appId` string COMMENT '应用id', ,
             `vs` string COMMENT '版本号',
             `ts` bigint COMMENT '启动时间', ,
             `area` string COMMENT '城市' */

        String mid = "mid_" + RandomNum.getRandInt(1, 200);
        String uid = "" + RandomNum.getRandInt(1, 1000);
        String os = osOptionGroup.getRandomOpt().getValue();
        String appid = this.appId;
        String area = areaOptionGroup.getRandomOpt().getValue();
        String vs = vsOptionGroup.getRandomOpt().getValue();
        //long ts= logDateUtil.getRandomDate().getTime();
        String ch = os.equals("ios") ? "appstore" : channelOptionGroup.getRandomOpt().getValue();

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", "startup");
        jsonObject.put("mid", mid);
        jsonObject.put("uid", uid);
        jsonObject.put("os", os);
        jsonObject.put("appid", appid);
        jsonObject.put("area", area);
        jsonObject.put("ch", ch);
        jsonObject.put("vs", vs);
        return jsonObject.toJSONString();
    }

    //获取数据
    private static void genLog() {
        JsonMocker jsonMocker = new JsonMocker();
        jsonMocker.startupNum = 1000000;

        for (int i = 0; i < jsonMocker.startupNum; i++) {

            //初始化Start数据
            String startupLog = jsonMocker.initStartsupLog();
            jsonMocker.sendLog(startupLog);

            //初始化Event数据
            while (!jsonMocker.isQuitGroup.getRandomOpt().getValue()) {

                String eventLog = jsonMocker.initEventLog(startupLog);
                jsonMocker.sendLog(eventLog);
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    //发送数据
    private void sendLog(String log) {
        LogUploader.sendLogStream(log);
    }


}
