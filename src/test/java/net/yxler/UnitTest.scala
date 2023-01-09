package net.yxler


import io.github.youngxinler.sparkRedisRdbParser.SparkContextWrapper.{RedisKeyValuePairWrapper, SparkContextRedisRdbFileWrapper}
import org.apache.spark.{SparkConf, sql}
import org.scalatest.{FunSuite, Matchers}



class UnitTest extends FunSuite with Matchers{
  test("sparkTest") {
    val conf = new SparkConf().set("spark.master", "local[*]")
      .registerKryoClasses(Array(Class.forName("net.whitbeck.rdbparser.KeyValuePair")));
    val spark = new sql.SparkSession.Builder().config(conf).appName(this.getClass.getSimpleName.stripSuffix("$")).getOrCreate()
    val path = "file:///Users/yxler/code/redis<=6.2.8-test.rdb"

    val kv = spark.sparkContext.redisRdbFile(path).selectKV().collect()
    val hash = spark.sparkContext.redisRdbFile(path).selectHash().collect()
    val set = spark.sparkContext.redisRdbFile(path).selectSet().collect()
    val list = spark.sparkContext.redisRdbFile(path).selectList().collect()
    val zset = spark.sparkContext.redisRdbFile(path).selectZSet().collect()


    assert(kv.length == 1)
    assert(kv(0)._1 == "hello1" && kv(0)._2 == "world1")

    assert(hash.length == 1)
    assert(hash(0)._1 == "hash1" && hash(0)._2 == Map("f1" -> "v1", "f2" -> "v2"))


    assert(set.length == 1)
    assert(set(0)._1 == "set1" && set(0)._2 == Set("v1", "v2"))

    assert(list.length == 1)
    assert(list(0)._1 == "list1" && list(0)._2 == List("v1", "v2"))

    assert(zset.length == 1)
    val assertZSetRes = Set(("v1", 1.0D), ("v2", 2.0D))
    assert(zset(0)._1 == "zset1" && zset(0)._2 == assertZSetRes)
  }
}
