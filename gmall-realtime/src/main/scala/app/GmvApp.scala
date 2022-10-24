package app

/**
  * @author wsl
  * @version 2021-01-09
  *          不用去重，直接脱敏保存
  */
import bean.OrderInfo
import com.alibaba.fastjson.JSON
import com.guigu.GmallConstant
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.phoenix.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import untils.MyKafkaUtil

object GmvApp {

  def main(args: Array[String]): Unit = {

    //1.创建环境
    val conf: SparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //sparkstreaming中chreckpoint保留数据和计算逻辑，实现累计用 StreamingContext.getActiveOrCreate()

    //2.消费Kafka订单主题数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.ORDER_INFO_TOPIC, ssc)

    //3.将每行数据转换为样例类对象,补充时间,数据脱敏(手机号)
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(record => {
      //a.将value转换为样例类
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      //b.取出创建时间 yyyy-MM-dd HH:mm:ss
      val create_time: String = orderInfo.create_time
      //c.给时间重新赋值
      val dateTimeArr: Array[String] = create_time.split(" ")
      orderInfo.create_date = dateTimeArr(0)
      orderInfo.create_hour = dateTimeArr(1).split(":")(0)
      //d.手机号脱敏138xxxxxxx1000   138xxxxxxxxxxx
      val consignee_tel: String = orderInfo.consignee_tel
      val tuple: (String, String) = consignee_tel.splitAt(4)
      orderInfo.consignee_tel = tuple._1 + "*******"
      //e.返回结果
      orderInfo
    })

    //Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR")
    //4.写入Phoenix
    orderInfoDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL2021_ORDER_INFO",
        classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase()),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //6.开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
