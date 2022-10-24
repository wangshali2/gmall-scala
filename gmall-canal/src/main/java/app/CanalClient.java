package app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.guigu.GmallConstant;
import untils.MyKafkaSender;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

/**
 * @author wsl
 * @version 2021-01-09
 */
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {

        //获取Canal连接对象
        CanalConnector canalConnector = CanalConnectors
                .newSingleConnector(new InetSocketAddress("hadoop102", 11111),
                        "example",
                        "",
                        "");

        //抓取数据并解析
        while (true) {

            //1.连接canal
            canalConnector.connect();
            //2.指定消费的数据表
            canalConnector.subscribe("gmallss.*");
            //3.抓取数据
            Message message = canalConnector.get(100);

            //4.判空,避免频繁访问canal
            List<CanalEntry.Entry> entries = message.getEntries();
            if (entries.size() <= 0) {
                System.out.println("当前没有数据！休息一会！");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {

                //有数据：解析entries
                for (CanalEntry.Entry entry : entries) {

                    //判断,只去RowData类型
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {

                        //获取表名用于决定数据发往哪个topic
                        String tableName = entry.getHeader().getTableName();
                        //获取序列化的数据
                        ByteString storeValue = entry.getStoreValue();
                        //反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        //反序列后得到1.事件类型EventType
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //2.获取rowDatasList
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        //根据表名以及事件类型处理数据rowDatasList
                        handler(tableName, eventType, rowDatasList);
                    }

                }

            }
        }
    }

    /**
     * 根据表名(订单表)以及事件类型处理数据rowDatasList
     *
     * @param tableName    表名
     * @param eventType    事件类型
     * @param rowDatasList 数据集合
     */
    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //  抓取订单表数据 。订单表,只需要新增数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            //遍历行集
            sendToKafka(rowDatasList, GmallConstant.ORDER_INFO_TOPIC);
        } else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            sendToKafka(rowDatasList, GmallConstant.ORDER_DETAIL_TOPIC);

        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))) {
            sendToKafka(rowDatasList, GmallConstant.GMALL_USER_INFO);
        }

    }

    private static void sendToKafka(List<CanalEntry.RowData> rowDatasList, String topic) {
        for (CanalEntry.RowData rowData : rowDatasList) {
            //创建JSON对象,用于存放一行数据
            JSONObject jsonObject = new JSONObject();

            //遍历列集
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(), column.getValue());
            }
            //测试打印并将数据发送至kafka主题
            System.out.println(jsonObject.toString());

            //手动制作延迟
         /*   try {
                Thread.sleep(new Random().nextInt(5)*100L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }*/

            //创建生产者发送数据
            MyKafkaSender.send(topic, jsonObject.toString());
        }
    }
}




