package io.github.manuzhang.dag.utils

import javax.annotation.Nullable

import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._

trait Collector extends java.io.Serializable {

  def append(key: String, value: String): Unit

  def save(key: String, value: String): Unit

  def save(key: String, value: Iterator[String]): Unit

  @Nullable
  def load(key: String): String

  def loadRange(key: String): Iterator[String]

  def remove(key: String): Unit
}

object Collector {

  def getDataAndSchemaKey(appId: String, dagId: String, nodeId: String, outputPort: Int): String = {
    s"$appId:$dagId:$nodeId:$outputPort"
  }

  def getSchemaKey(appId: String, dagId: String, nodeId: String, outputPort: Int): String = {
    getSchemaKey(getDataAndSchemaKey(appId, dagId, nodeId, outputPort))
  }

  def getVisKey(appId: String, dagId: String, nodeId: String, outputPort: Int): String = {
    val prefix = getDataAndSchemaKey(appId, dagId, nodeId, outputPort)
    s"$prefix:vis"
  }

  def getSchemaKey(keyPrefix: String): String = {
    s"$keyPrefix:schema"
  }

  def getDataKey(appId: String, dagId: String, nodeId: String, outputPort: Int): String = {
    getDataKey(getDataAndSchemaKey(appId, dagId, nodeId, outputPort))
  }

  def getDataKey(keyPrefix: String): String = {
    s"$keyPrefix:data"
  }
}

class RedisCollector extends Collector {

  private val EXPIRATION_IN_SECONDS = 60 * 60 * 24 * 7
  private val host = System.getProperty("redis.host")
  private val port = Option(System.getProperty("redis.port")).getOrElse("6379").toInt
  private val jedis = new Jedis(host, port)

  override def append(key: String, value: String): Unit = {
    jedis.rpush(key, value)
    jedis.expire(key, EXPIRATION_IN_SECONDS)
  }

  override def save(key: String, value: String): Unit = {
    jedis.setex(key, EXPIRATION_IN_SECONDS, value)
  }

  override def save(key: String, value: Iterator[String]): Unit = {
    remove(key)
    value.foreach { v =>
      jedis.rpush(key, v)
    }
    jedis.expire(key, EXPIRATION_IN_SECONDS)
  }

  def remove(key: String): Unit = {
    jedis.del(key)
  }

  @Nullable
  override def load(key: String): String = {
    jedis.get(key)
  }

  override def loadRange(key: String): Iterator[String] = {
    val data = jedis.lrange(key, 0, -1)
    data.asScala.toIterator
  }

  def close(): Unit = {
    jedis.close()
  }

}
