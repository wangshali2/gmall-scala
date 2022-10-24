package app

import java.text.SimpleDateFormat
import java.util.Date

import bean.StartUpLog
import com.alibaba.fastjson.JSON
import com.guigu.GmallConstant
import handler.H
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import untils.MyKafkaUtil

/**
  * @author wsl
  * @version 2021-01-12
  */
object D {
  def main(args: Array[String]): Unit = {
     //1.环境
    val conf: SparkConf = new SparkConf().setAppName("xx").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))
     //2.获取kafka数据
    val data: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_STARTUP,ssc)
     //3.转换样例类
    val start: DStream[StartUpLog] = data.map(record => {
      val json: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
      val ts: Long = json.ts
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
      val arr: Array[String] = sdf.format(new Date(ts)).split(" ")
      json.logDate = arr(0)
      json.logHour = arr(1)
      json
    })
    start.print()
     //4.跨批次去重
    val value: DStream[StartUpLog] = H.fiter(start,ssc.sparkContext)
     //5.同批次去重
    val value2: DStream[StartUpLog] = H.filter2(value)
     //6.保存到redis
    H.save(value2)
     //7.
    ssc.start()
    ssc.awaitTermination()
  }
}
