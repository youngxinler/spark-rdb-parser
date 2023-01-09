# Spark Read RDB File Parser

## Overview
spark read redis rdb file to a RDD, and select Redis Type to cast. 
 

```java
import net.yxler.sparkRedisRdbParser.SparkContextWrapper.{RedisKeyValuePairWrapper, SparkContextRedisRdbFileWrapper}

// must register Kyro Classs include "net.whitbeck.rdbparser.KeyValuePair"
val conf = new SparkConf().set("spark.master", "local[*]")
    .registerKryoClasses(Array(Class.forName("net.whitbeck.rdbparser.KeyValuePair")));
val spark = new sql.SparkSession.Builder().config(conf).appName(this.getClass.getSimpleName.stripSuffix("$")).getOrCreate()

// Get All RDB Key Value
val rdd: RDD[KeyValuePair] = spark.sparkContext.redisRdbFile(path)

// tuple2 first value is redis key, second value is redis value.

// Get All Redis String Type
val kvRDD: RDD[(String, String)] = spark.sparkContext.redisRdbFile(path).selectKV()

// Get All Redis Hash Type, map key is hash field, map value is the hash fied value 
val hash: RDD[(String, Map[String, String])] = spark.sparkContext.redisRdbFile(path).selectHash()

// Get All Redis Set Type
val set: RDD[(String, Set[String])] = spark.sparkContext.redisRdbFile(path).selectSet()

// Get All Redis List Type
val list: RDD[(String, List[String])] = spark.sparkContext.redisRdbFile(path).selectList()

// Get All Redis ZSet Type,  Double is the sorted set value's score
val zset: RDD[(String, Set[String, Double])] = spark.sparkContext.redisRdbFile(path).selectZSet()
```


