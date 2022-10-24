package app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import bean.{CouponAlertInfo, EventLog}
import com.alibaba.fastjson.JSON

import com.guigu.GmallConstant
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import untils.{MyEsUtil, MyKafkaUtil}

import scala.util.control.Breaks._

object AlertApp {

  def main(args: Array[String]): Unit = {

    //1.创建执行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //2.读取Kafka事件日志主题数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_EVENT, ssc)

    //3.将每行数据转换为样例类对象
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val logDStream: DStream[EventLog] = kafkaDStream.map(record => {

      //a.转换为样例类对象
      val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])

      //b.补充时间字段
      val ts: Long = eventLog.ts
      val dateHourArr: Array[String] = sdf.format(new Date(ts)).split(" ")
      eventLog.logDate = dateHourArr(0)
      eventLog.logHour = dateHourArr(1)

      //c.返回数据
      eventLog
    })

    //4.开窗
    val windowDStream: DStream[EventLog] = logDStream.window(Minutes(5))

    //5.按照Mid分组  log => (mid, log)   => (mid, iter(log))
    val midToLogIter: DStream[(String, Iterable[EventLog])] = windowDStream.map(log => (log.mid, log)).groupByKey()

    //6.筛选组内数据
    val alertInfoDStream: DStream[CouponAlertInfo] = midToLogIter.map { case (mid, iter) =>

      //创建Set用于存放领券的Uid,用于去重
      val uids: util.HashSet[String] = new util.HashSet[String]()
      //创建Set用于存放商品ID
      val itemIds = new util.HashSet[String]()
      //创建集合用于存放事件类型
      val events = new util.ArrayList[String]()

      //创建标志位用于表示是否存在浏览商品行为
      var noClick = true

      //遍历迭代器
      breakable {
        iter.foreach(log => {
          //提取数据中的行为字段
          val evid: String = log.evid
          //添加事件类型进集合
          events.add(evid)
          if ("coupon".equals(evid)) {
            //领券行为,将用户ID添加至集合
            uids.add(log.uid)
            itemIds.add(log.itemid)
          } else if ("clickItem".equals(evid)) {
            //浏览商品行为,将标志位改为false
            noClick = false
            break()
          }
        })
      }

      //判断用户ID的个以及是否存在浏览商品行为决定是否生成预警日志
      //      if (uids.size() >= 3 && noClick){
      //        CouponAlertInfo
      //      } else {
      //        null
      //      }

      (uids.size() >= 3 && noClick, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
  }.filter(_._1).map(_._2)
    //upload{"area":"heilongjiang","uid":"122","itemid":4,"npgid":23,"evid":"coupon","os":"ios","pgid":28,"appid":"gmall2019","mid":"mid_115","type":"event"}
    //upload{"area":"heilongjiang","uid":"122","itemid":49,"npgid":22,"evid":"coupon","os":"ios","pgid":49,"appid":"gmall2019","mid":"mid_115","type":"event"}

    //CouponAlertInfo(mid_34,[2841, 4756, 4769],[33, 45, 34, 9, 16, 42],
    // [coupon, addFavor, coupon, addComment, addComment, coupon, coupon, addComment, addFavor, addComment, addComment, addComment, coupon, coupon, addCart, addComment, addCart, coupon],1610340845152)

    alertInfoDStream.cache()
    alertInfoDStream.print(100)

    //7.写入ES
    alertInfoDStream.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {

        //遍历迭代器补充DocID字段  info => ((mid-minut),info)
        val docIter: Iterator[(String, CouponAlertInfo)] = iter.map(info => {
          val minut: Long = info.ts / 1000 / 60
          (s"${info.mid}-$minut", info)
        })

        //执行批量写入数据
        //  "_index" : "gmall_coupon_alert_2021-01-11",
        //  "_id" : "mid_154-26839140",
        val date: String = sdf.format(new Date(System.currentTimeMillis())).split(" ")(0)
        //insertBulk(indexName: String, docList: List[(String, Any)])  iters里只有any，需要补充docid
        MyEsUtil.insertBulk(s"${GmallConstant.ALERT_INDEX_PRE}_$date", docIter.toList)
      })
    })

    //8.启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}
