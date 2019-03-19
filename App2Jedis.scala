package com.Utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * 连接池
  */
object App2Jedis {
  val config = new JedisPoolConfig()
  // 设置最大连接
  config.setMaxTotal(20)
  config.setMaxIdle(10)
  // 连接等待超时时间
  private val pool = new JedisPool(config,"192.168.28.131",6379,10000,"123")
  // 获取连接
  def getConnection():Jedis={
    pool.getResource
  }
}
