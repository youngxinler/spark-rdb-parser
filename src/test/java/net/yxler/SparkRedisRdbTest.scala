package net.yxler

import net.whitbeck.rdbparser.{KeyValuePair, ValueType}
import net.yxler.sparkRedisRdbParser.RedisRdbNewInputFormat
import org.apache.spark.{SparkConf, sql}
import net.yxler.sparkRedisRdbParser.SparkContextWrapper.SparkContextRedisRdbFileWrapper

object SparkRedisRdbTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.master", "local[*]")
      .registerKryoClasses(Array(Class.forName("net.whitbeck.rdbparser.KeyValuePair")));
    val spark = new sql.SparkSession.Builder().config(conf).appName(this.getClass.getSimpleName.stripSuffix("$")).getOrCreate()
    val path = "file:///Users/yxler/data/redis-rdb/test.rdb"
//    val newRdd = spark.sparkContext.newAPIHadoopFile(path, classOf[RedisRdbNewInputFormat], classOf[String], classOf[KeyValuePair], spark.sparkContext.hadoopConfiguration)
//    newRdd
//      .take(100)
//      .foreach(x => {
//        println(x)
//      })

    spark.sparkContext.redisRdbFile(path)
      .map(x => {
        if (x.getValueType == ValueType.HASH) {
          val fields = x.getValues
          println(fields)
        }

      })
      .foreach(println)
  }
}
