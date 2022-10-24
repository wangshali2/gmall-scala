package app

import bean.UserInfo
import com.alibaba.fastjson.JSON
import com.guigu.GmallConstant
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import untils.{MyKafkaUtil, RedisUtil}

/**
  * @author wsl
  * @version 2021-01-12
  *          String类型，有更新直接覆盖。set：保留所有更新的数据，不知道取哪条。
  *          key:userinfo:
  */
//将用户表新增及变化数据缓存至Redis
object UserInfoApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.消费Kafka用户主题数据创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_USER_INFO, ssc)

    //4.取出Value
    val userJsonDStream: DStream[String] = kafkaDStream.map(_.value())

    //5.将用户数据写入Redis
    userJsonDStream.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {
        //a.获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //b.写库
        iter.foreach(userJson => {
          val userInfo: UserInfo = JSON.parseObject(userJson, classOf[UserInfo])
          val redisKey = s"UserInfo:${userInfo.id}"
          jedisClient.set(redisKey, userJson)
        })
        //c.归还连接
        jedisClient.close()
      })

    })

    //开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}

