package io.github.youngxinler.sparkRedisRdbParser

import net.whitbeck.rdbparser.{KeyValuePair, RdbParser, ValueType}

import scala.collection.JavaConverters._
import scala.collection.mutable

object KeyValuePairWrapper {
  implicit class WrapperCore(val kv: KeyValuePair) {
    def asKV(): (String, String) = {
      (new String(kv.getKey), new String(kv.getValues.get(0)))
    }

    def asList(): (String, List[String]) = {
      val key = new String(kv.getKey)
      val list = kv.getValues.asScala.map(new String(_)).toList
      (key, list)
    }

    def asSet(): (String, Set[String]) = {
      val key = new String(kv.getKey)
      val set = kv.getValues.asScala.map(new String(_)).toSet
      (key, set)
    }

    def asZSet(): (String, Set[(String, Double)]) = {
      val key = new String(kv.getKey)
      val zset = new mutable.HashSet[(String, Double)]()
      for (i <- Range(0, kv.getValues.size(), 2)) {
        val field = new String(kv.getValues.get(i))
        val score =
          if (kv.getValueType == ValueType.SORTED_SET2)
            RdbParser.parseSortedSet2Score(kv.getValues.get(i + 1))
          else
            RdbParser.parseSortedSetScore(kv.getValues.get(i + 1))
        zset.add((field, score))
      }
      (key, zset.toSet)
    }

    def asHash(): (String, Map[String, String]) = {
      val key = new String(kv.getKey)
      val map = new mutable.HashMap[String, String]()
      for (i <- Range(0, kv.getValues.size(), 2)) {
        val field = new String(kv.getValues.get(i))
        val value = new String(kv.getValues.get(i + 1))
        map.put(field, value)
      }
      (key, map.toMap)
    }
  }
}
