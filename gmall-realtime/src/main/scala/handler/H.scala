package handler

import java.text.SimpleDateFormat
import java.{lang, util}
import java.util.Date

import bean.StartUpLog
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis
import untils.RedisUtil

/**
  * @author wsl
  * @version 2021-01-12
  */
object H {
  def save(value2: DStream[StartUpLog]) = {
    value2.foreachRDD(rdd=>{
    rdd.foreachPartition(iter=>{
      val client: Jedis = RedisUtil.getJedisClient
      iter.foreach(log=>{
        val key=s"DAU:${log.logDate}"
        client.sadd(key,log.mid)

      })
      client.close()
    })

    })
  }

  def filter2(start: DStream[StartUpLog]) = {
    val value: DStream[((String, String), Iterable[StartUpLog])] = start.map {
      log => ((log.mid, log.logDate), log)
    }.groupByKey()
    value.flatMap {
      case (k, iter) => {
        iter.toList.sortWith(_.ts < _.ts).take(1)
      }
    }


  }

  def fiter(start: DStream[StartUpLog], sc: SparkContext) = {
    //    start.filter(log=>{
    //      val client: Jedis = RedisUtil.getJedisClient
    //      val key=s"DAU:${log.logDate}"
    //      val boolean: lang.Boolean = client.sismember(key,log.mid)
    //      client.close()
    //      !boolean
    //    })


    //    start.mapPartitions(partition => {
    //      val client: Jedis = RedisUtil.getJedisClient
    //      val logs: Iterator[StartUpLog] = partition.filter(log => {
    //        val key = s"DAU:${log.logDate}"
    //        val boolean: lang.Boolean = client.sismember(key, log.mid)
    //        !boolean
    //      })
    //      client.close()
    //
    //      logs
    //    })


    val value: DStream[StartUpLog] = start.transform(rdd => {
      val sdf = new SimpleDateFormat("yyyy-MM-dd -HH")
      val client: Jedis = RedisUtil.getJedisClient
      var key = s"DAU:${new Date(System.currentTimeMillis())}"
      val mids: util.Set[String] = client.smembers(key)
      val bd: Broadcast[util.Set[String]] = sc.broadcast(mids)
      client.close()
      rdd.filter(log => {
        !bd.value.contains(log.mid)
      })
    })
    value
  }

}
