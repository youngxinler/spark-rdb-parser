package net.yxler.sparkRedisRdbParser

import net.whitbeck.rdbparser.{KeyValuePair, ValueType}
import net.yxler.sparkRedisRdbParser.KeyValuePairWrapper.WrapperCore
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.util.regex.Pattern
import scala.language.postfixOps
import scala.reflect.ClassTag

object SparkContextWrapper {
  implicit class SparkContextRedisRdbFileWrapper(val sc: SparkContext) {
    def redisRdbFile(path: String,  keyRegexMatch: String = null, typeFilter: List[ValueType] = List.empty): RDD[KeyValuePair] = {
      val allRdb = sc.newAPIHadoopFile(path, classOf[RedisRdbNewInputFormat], classOf[String], classOf[KeyValuePair], sc.hadoopConfiguration)

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
      rdd.flatMap(kv => {
        if (kv.getValueType == ValueType.VALUE)
          Option.apply(kv.asKV())
        else
          Option.empty[(String, String)]
      })
    }

    def selectList(): RDD[(String, List[String])] = {
      rdd.flatMap(kv => {
        if (kv.getValueType == ValueType.LIST || kv.getValueType == ValueType.ZIPLIST || kv.getValueType == ValueType.QUICKLIST) {
          Option.apply(kv.asList())
        } else {
          Option.empty[(String, List[String])]
        }
      })
    }

    def selectHash(): RDD[(String, Map[String, String])] = {
      rdd.flatMap(kv => {
        if (kv.getValueType == ValueType.HASH || kv.getValueType == ValueType.HASHMAP_AS_ZIPLIST)
          Option.apply(kv.asHash())
        else
          Option.empty[(String, Map[String, String])]
      })
    }

    def selectSet(): RDD[(String, Set[String])] = {
      rdd.flatMap(kv => {
        if (kv.getValueType == ValueType.SET || kv.getValueType == ValueType.INTSET)
          Option.apply(kv.asSet())
        else
          Option.empty[(String, Set[String])]
      })
    }

    def selectZSet(): RDD[(String, Set[(String, Double)])] = {
      rdd.flatMap(kv => {
        if (kv.getValueType == ValueType.SORTED_SET || kv.getValueType == ValueType.SORTED_SET2 || kv.getValueType == ValueType.SORTED_SET_AS_ZIPLIST)
          Option.apply(kv.asZSet())
        else {
          Option.empty[(String, Set[(String, Double)])]
        }
      })
    }
  }
}
