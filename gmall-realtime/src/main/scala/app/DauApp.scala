package app

import java.text.SimpleDateFormat
import java.util.Date

import bean.StartUpLog
import com.alibaba.fastjson.JSON
import com.guigu.GmallConstant
import handler.DauHandler
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._
import untils.MyKafkaUtil

object DauApp {

  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //2.读取Kafka数据创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_STARTUP, ssc)

    //3.将每行数据转换为样例类对象(补充时间字段)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.map(record => {

      //a.将Value转换为样例类对象
      val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])

      //b.获取数据中的Ts字段并进行格式化
      val ts: Long = startUpLog.ts
      val dateHourStr: String = sdf.format(new Date(ts))
      val dateHourArr: Array[String] = dateHourStr.split(" ")

      //c.补充时间字段
      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)

      //d.返回结果
      startUpLog
    })

    //打印数据条数测试
    startUpLogDStream.cache()
    startUpLogDStream.print(10)
    startUpLogDStream.count().print()

    //4.跨批次去重,根据Redis中数据去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream, ssc.sparkContext)

    filterByRedisDStream.cache()
    filterByRedisDStream.print(10)
    filterByRedisDStream.count().print()

    //5.同批次去重
    val filterByMidDStream: DStream[StartUpLog] = DauHandler.filterByMid(filterByRedisDStream)

    filterByMidDStream.cache()
    filterByMidDStream.count().print()

    //6.将去重之后的数据Mid存入Redis
    DauHandler.saveMidToRedis(filterByMidDStream)

    //7.将去重之后的数据明细存入HBase(Phoenix)
    filterByMidDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL2021_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //8.启动任务
    ssc.start()
    ssc.awaitTermination()



  }
}
