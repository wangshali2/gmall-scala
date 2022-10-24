package handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis
import untils.RedisUtil

object DauHandler {

  /**
    * 同批次去重
    *
    * @param filterByRedisDStream 经过Redis跨批次去重之后的结果
    * @return
    */
  def filterByMid(filterByRedisDStream: DStream[StartUpLog]): DStream[StartUpLog] = {

    //1.转换数据结构
    val dateMidToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.map(startLog =>
      ((startLog.logDate, startLog.mid), startLog)
    )

    //2.按照Key分组
    val dateMidToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = dateMidToLogDStream.groupByKey()

    //3.对迭代器中的数据按照时间排序取第一条
    //    val value: DStream[((String, String), List[StartUpLog])] = dateMidToLogIterDStream.mapValues(iter => {
    //      iter.toList.sortWith(_.ts < _.ts).take(1)
    //    })
    //    val value1: DStream[List[StartUpLog]] = value.map(_._2)
    //    val value2: DStream[StartUpLog] = value1.flatMap(x => x)

    //结果是否需要保留Key,结果是否需要压平操作
    //map:不需要Key,也不需要压平
    //flatMap:不需要Key,但是需要压平
    //mapValues:需要Key,但是不需要压平
    //flatMapValues:需要Key,同时也需要压平
    val value3: DStream[StartUpLog] = dateMidToLogIterDStream.flatMap { case ((date, mid), iter) =>
      iter.toList.sortWith(_.ts < _.ts).take(1)
    }

    //4.最终返回数据
    value3
  }


  /**
    * 跨批次去重,根据Redis中数据去重
    *
    * @param startUpLogDStream 原始数据
    * @return 跨批次去重之后的数据
    */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog], sc: SparkContext): DStream[StartUpLog] = {

    //方案一：使用filter算子实现去重功能
    //    val value1: DStream[StartUpLog] = startUpLogDStream.filter(startUpLog => {
    //      //a.获取Redis连接
    //      val jedisClient: Jedis = RedisUtil.getJedisClient
    //      //b.查询Redis中是否已经存在当前数据的Mid
    //      val redisKey = s"DAU:${startUpLog.logDate}"
    //      val exist: lang.Boolean = jedisClient.sismember(redisKey, startUpLog.mid)
    //      //c.归还连接
    //      jedisClient.close()
    //      //d.返回结果
    //      !exist
    //    })

    //方案二：使用MapPartitions代替filter,减少连接的创建和释放
    //    val value2: DStream[StartUpLog] = startUpLogDStream.mapPartitions(iter => {
    //      //a.获取Redis连接
    //      val jedisClient: Jedis = RedisUtil.getJedisClient
    //      //b.遍历一个分区的数据,查询Redis中是否存在当前数据的Mid
    //      val logs: Iterator[StartUpLog] = iter.filter(startUpLog => {
    //        val redisKey = s"DAU:${startUpLog.logDate}"
    //        !jedisClient.sismember(redisKey, startUpLog.mid)
    //      })
    //      //c.释放连接
    //      jedisClient.close()
    //      //d.返回数据
    //      logs
    //    })

    //mapPartitions    有返回值  查询
    //transform      有返回值    查询  替代mapPartitions
    //foreachRdd     无返回值    保存

    //方案三：每个批次获取一次连接,访问一次Redis,使用广播变量发送至Executor
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val value3: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //a.获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //b.查询Redis中当天的数据
      val redisKey = s"DAU:${sdf.format(new Date(System.currentTimeMillis()))}"
      val mids: util.Set[String] = jedisClient.smembers(redisKey)
      //c.归还连接
      jedisClient.close()
      //d.广播变量
      val midsBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      //e.对RDD遍历,根据内存中的数据做过滤
      rdd.filter(startLog => {
        !midsBC.value.contains(startLog.mid)
      })
    })

    //    value1
    //    value2
    value3
  }

  /**
    * 将去重之后的数据Mid存入Redis
    * foreachRDD运行在Driver端，转换算子。foreachPartition是action算子
    * @param startUpLogDStream 两次去重之后的结果数据
    */
  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]): Unit = {

    //进一步：在处理一批RDD时，可以使用数据库连接池来重复使用连接对象，注意连接池必须是静态、懒加载的
    startUpLogDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        //a.获取连接  不能往上提，没有序列化。
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //b.遍历操作数据
        iter.foreach(startUpLog => {
          val redisKey = s"DAU:${startUpLog.logDate}"
          jedisClient.sadd(redisKey, startUpLog.mid)
        })
        //c.归还连接
        jedisClient.close()
      })
    })
  }
}