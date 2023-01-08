package net.yxler.sparkRedisRdbParser

import net.whitbeck.rdbparser.{KeyValuePair, ValueType}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.util.regex.Pattern
import java.util.stream.Collectors
import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.collection.JavaConverters._
import scala.collection.mutable

object SparkContextWrapper {
  implicit class SparkContextRedisRdbFileWrapper(val sc: SparkContext) {

    def redisRdbFile(path: String,  keyRegexMatch: String = null, typeFilter: List[ValueType] = List.empty): RDD[KeyValuePair] = {
      val allRdb = sc.newAPIHadoopFile("file:///Users/yxler/data/redis-rdb/test.rdb", classOf[RedisRdbNewInputFormat], classOf[String], classOf[KeyValuePair], sc.hadoopConfiguration)

      val filters = (key: String, pair: KeyValuePair) => {
        var isFilter = true

        if (keyRegexMatch != null && keyRegexMatch.nonEmpty) {
          isFilter = isFilter && Pattern.matches(keyRegexMatch, key)
        }

        if (typeFilter.nonEmpty) {
          isFilter = isFilter && typeFilter.contains(pair.getType)
        }
        isFilter
      }

      allRdb.filter(x => filters(x._1, x._2)).values
    }
  }



  implicit class RedisKeyValuePairWrapper[T <: KeyValuePair: ClassTag](val rdd: RDD[T]) {


    def selectKV(): RDD[(String, String)] = {
      rdd.map(kv => {
        if (kv.getValueType == ValueType.VALUE)
          (new String(kv.getKey), new String(kv.getValues.get(0)))
        else
          null
      })
    }

    def selectList(): RDD[(String, List[String])] = {
      rdd.map(kv => {
        if (kv.getValueType == ValueType.LIST || kv.getValueType == ValueType.ZIPLIST) {
          val key = new String(kv.getKey)
          val list = kv.getValues.asScala.map(new String(_)).toList
          (key, list)
        } else {
          null
        }
      })
    }

    def selectHash(): RDD[(String, Map[String, String])] = {
      rdd.map(kv => {
        if (kv.getValueType == ValueType.HASH || kv.getValueType == ValueType.HASHMAP_AS_ZIPLIST) {
          val key = new String(kv.getKey)
          var i = 0
          val map = new mutable.HashMap[String, String]()
          val bytesArray = kv.getValues
          while (i < bytesArray.size()) {
            map.put(new String(bytesArray.get(i++)), new String(bytesArray.get(i++)))
          }
        }
      })
    }


    def selectSet(): RDD[(String, Set[String])] = {

    }

    select
  }
}
