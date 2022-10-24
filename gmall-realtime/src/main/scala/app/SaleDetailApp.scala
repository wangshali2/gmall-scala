package app

import bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.alibaba.fastjson.JSON
import com.guigu.GmallConstant
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis
import untils.{MyKafkaUtil, RedisUtil}


import scala.collection.mutable.ListBuffer

/**
  * @author wsl
  * @version 2021-01-11
  */
//将OrderInfo与OrderDetail数据进行双流JOIN,并根据user_id查询Redis,补全用户信息
object SaleDetailApp {

  def main(args: Array[String]): Unit = {

    //1.todo 创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //2.todo 消费Kafka订单以及订单明细主题数据创建流
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.ORDER_INFO_TOPIC, ssc)
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.ORDER_DETAIL_TOPIC, ssc)

    //3.todo 将数据转换为样例类对象并转换结构为元祖 (order_id,orderinfo),(order_id,orderdetail)
    val orderIdToInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafkaDStream.map(record => {
      //a.将value转换为样例类
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      //b.取出创建时间 yyyy-MM-dd HH:mm:ss
      val create_time: String = orderInfo.create_time
      //c.给时间重新赋值
      val dateTimeArr: Array[String] = create_time.split(" ")
      orderInfo.create_date = dateTimeArr(0)
      orderInfo.create_hour = dateTimeArr(1).split(":")(0)
      //d.数据脱敏
      val consignee_tel: String = orderInfo.consignee_tel
      val tuple: (String, String) = consignee_tel.splitAt(4)
      orderInfo.consignee_tel = tuple._1 + "*******"
      //e.返回结果
      (orderInfo.id, orderInfo)
    })

    val orderIdToDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.map(record => {
      //a.转换为样例类
      val detail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      //b.返回数据
      (detail.order_id, detail)
    })

    //(5,(OrderInfo(5,6,Uwzirh,EJIpxtBBcJmkmHxsuTmB,1374*******,1,1,5,,59.0,,wDBXhVfNAmLTktnlqWKM,2021-01-09 05:38:37,,,,9245329879,,2021-01-09,05),
    // OrderDetail(10,5,荣耀10青春版 幻彩渐变 2400万AI自拍 全网通版4GB+64GB 渐变蓝 移动联通电信4G全面屏手机 双卡双待,1,2220.0,http://FQmPoWUIEYfFJFYeygmQrkcHcqxobpviJJmteqsR,1)))
    //(5,(OrderInfo(5,6,Uwzirh,EJIpxtBBcJmkmHxsuTmB,1374*******,1,1,5,,59.0,,wDBXhVfNAmLTktnlqWKM,2021-01-09 05:38:37,,,,9245329879,,2021-01-09,05),
    // OrderDetail(11,5,小米Play 流光渐变AI双摄 4GB+64GB 梦幻蓝 全网通4G 双卡双待 小水滴全面屏拍照游戏智能手机,4,1442.0,http://chCtjNpmmARJTqWNaORQExCrclYiWiaFWTDjmJxs,3)))
    //  4.todo 双流JOIN(普通JOIN)不考虑网络延迟(orderid, (OrderInfo, OrderDetail))  会丢数据
    /*  val value: DStream[(String, (OrderInfo, OrderDetail))] = orderIdToInfoDStream.join(orderIdToDetailDStream)
      value.print(100)
  */


    //4.todo 双流join

    val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderIdToInfoDStream.fullOuterJoin(orderIdToDetailDStream)

    //5.todo 加缓存处理JOIN之后的数据
    val noUserSaleDetailDStream: DStream[SaleDetail] = fullJoinDStream.mapPartitions(iter => {

      //5.1 获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //创建集合用于存放join上的数据
      val details = new ListBuffer[SaleDetail]

      //不可序列化  样例类转换为json对象的格式化对象
      implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

      //5.2 遍历iter,做数据处理
      iter.foreach { case ((orderId, (infoOpt, detailOpt))) =>

        //rediskey
        val infoRedisKey = s"OrderInfo:$orderId"
        val detailRedisKey = s"OrderDetail:$orderId"

        //a.todo infoOpt不为空  S
        if (infoOpt.isDefined) {

          //取出info
          val orderInfo: OrderInfo = infoOpt.get

          //a.1 todo detailOpt不为空   S  ，join上
          if (detailOpt.isDefined) {
            //取出detail
            val orderDetail: OrderDetail = detailOpt.get
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            //添加至集合
            details += saleDetail
            //details.append()
          }

          //a.2 todo 将info数据写入redis
          // val infoStr: String = JSON.toJSONString(orderInfo)//编译通不过，样例类转成json不行，反过来可以。
          import org.json4s.native.Serialization
          implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
          //序列化成字符串
          val infoStr: String = Serialization.write(orderInfo)
          jedisClient.set(infoRedisKey, infoStr)
          jedisClient.expire(infoRedisKey, 100)

          //a.3 todo 查询detail缓存数据
          if (jedisClient.exists(detailRedisKey)) {
            //返回所有元素   相同订单的不同商品
            val detailJsonSet = jedisClient.smembers(detailRedisKey)
            //没有直接遍历，需要隐式转换
            import scala.collection.JavaConverters._
            detailJsonSet.asScala.foreach(detailJson => {
              //转换为样例类对象,并创建SaleDetail存入集合
              val detail: OrderDetail = JSON.parseObject(detailJson, classOf[OrderDetail])
              details += new SaleDetail(orderInfo, detail)
            })
          }

        } else {  //b.infoOpt为空

          val orderDetail: OrderDetail = detailOpt.get
          //b.1 查询缓存Info数据，存在
          if (jedisClient.exists(infoRedisKey)) {

            val infoJson: String = jedisClient.get(infoRedisKey)
            //转换数据为样例类对象
            val orderInfo: OrderInfo = JSON.parseObject(infoJson, classOf[OrderInfo])
            //创建SaleDetail存入集合
            details += new SaleDetail(orderInfo, orderDetail)

          } else {   //b.2 查询Redis中没有Info数据

            val detailStr: String = Serialization.write(orderDetail)
            //写入Redis
            jedisClient.sadd(detailRedisKey, detailStr)
            jedisClient.expire(detailRedisKey, 100)
          }
        }

      }

      //5.3 归还连接
      jedisClient.close()

      //5.4 最终返回值
      details.iterator

    })


    //测试
    noUserSaleDetailDStream.print(100)

    //6.todo 根据UserID查询Redis中的数据,补全用户信息
    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetailDStream.mapPartitions(iter => {

      //获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient

      //查询Redis补充信息
      val details: Iterator[SaleDetail] = iter.map(saleDetail => {
        val userRedisKey = s"UserInfo:${saleDetail.user_id}"
        //查询数据
        val userJson: String = jedisClient.get(userRedisKey)
        //将用户数据转换为样例类对象
        val userInfo: UserInfo = JSON.parseObject(userJson, classOf[UserInfo])
        //补全信息
        saleDetail.mergeUserInfo(userInfo)
        //返回数据
        saleDetail
      })

      //归还连接
      jedisClient.close()

      //返回数据
      details
    })

    saleDetailDStream.print(100)



    //开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}

