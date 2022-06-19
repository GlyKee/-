# 项目介绍

## 项目需求

1、用户行为数据采集平台搭建

2、业务数据采集平台搭建

3、数据仓库维度建模

4、分析,设备、会员、商品、地区、活动等电商核心主题,统计的报表指标近100个。完全对比中型公司。

5、采用即席查询工具,随时进行指标分析

6、对集群性能进行监控,发生异常需要报警。

7、元数据管理

8、质量监控

## 技术选型

技术选型主要考虑因素:数据量大小、业务需求、行业内经验、技术成熟度、开发维护成本、总成本预算

数据采集传输:Flume,Kafka,Sqoop

数据存储:MySql,HDFS,HBase,Redis

数据计算:Hive,Spark

数据查询:Presto,Kylin

任务调度:Azkaban

集群监控:Zabbix

元数据管理:Alas
# 流程
![数仓流程图](https://user-images.githubusercontent.com/103411715/174213438-3502163c-ba0c-429d-a15b-c0ccc717575d.png)
不直接用flume而是用flume-kafka-flume的原因：1.解耦。只用flume的话数据就只能传到特定的地点（例如hdfs），但是用kafka后数据比较容易被多方使用。2.异步。近期的数据一般比较重要，然kafka作为临时存储，等待使用时可以直接获取。
## 数据格式

日志数据分为普通日志和启动日志。普通日志包含common（用户信息）+actions（用户动作，例如添加购物车）+display（页面具体模块，例如查询模块）+page（页面信息）

```
{
  "common": {                  -- 公共信息
    "ar": "230000",              -- 地区编码
    "ba": "iPhone",              -- 手机品牌
    "ch": "Appstore",            -- 渠道
    "is_new": "1",--是否首日使用，首次使用的当日，该字段值为1，过了24:00，该字段置为0。
	"md": "iPhone 8",            -- 手机型号
    "mid": "YXfhjAYH6As2z9Iq", -- 设备id
    "os": "iOS 13.2.9",          -- 操作系统
    "uid": "485",                 -- 会员id
    "vc": "v2.1.134"             -- app版本号
  },
"actions": [                     --动作(事件)  
    {
      "action_id": "favor_add",   --动作id
      "item": "3",                   --目标id
      "item_type": "sku_id",       --目标类型
      "ts": 1585744376605           --动作时间戳
    }
  ],
  "displays": [
    {
      "displayType": "query",        -- 曝光类型
      "item": "3",                     -- 曝光对象id
      "item_type": "sku_id",         -- 曝光对象类型
      "order": 1,                      --出现顺序
      "pos_id": 2                      --曝光位置
    },
    {
      "displayType": "promotion",
      "item": "6",
      "item_type": "sku_id",
      "order": 2, 
      "pos_id": 1
    },
    {
      "displayType": "promotion",
      "item": "9",
      "item_type": "sku_id",
      "order": 3, 
      "pos_id": 3
    }
  ],
  "page": {                       --页面信息
    "during_time": 7648,        -- 持续时间毫秒
    "item": "3",                  -- 目标id
    "item_type": "sku_id",      -- 目标类型
    "last_page_id": "login",    -- 上页类型
    "page_id": "good_detail",   -- 页面ID
    "sourceType": "promotion"   -- 来源类型
  },
"err":{                     --错误
"error_code": "1234",      --错误码
    "msg": "***********"       --错误信息
},
  "ts": 1585744374423  --跳入时间戳
}

```

## 集群规模说明

假设：

(1)每天日活跃用户100万,每人一天平均100条:100万*100条=1亿条

(2)每条日志1K左右,每天1亿条:100000000/1024/1024=约100G 

(3)半年内不广容服务器来算:100G*180天=约18T 

(4)保存3副本:18T*3=54T 

  (5)预留20%-30%Bf=54T/0.7=77T 

(6)算到这:约10台服务器（每台8T磁盘128G内存）

上述是集群规模的理论选择。**事实上，本次项目只是用三台虚拟机，内存1G，磁盘40G。**

# 准备

## Hadoop测试

测试HDFS前的IO速度前，先测试各个节点的网络速度。通过

```
python -m SimpleHTTPServer 8000
```

将/opt/software目录暴露在8000端口，再从该端口下载文件，查看三个节点的下载速度均为10MB/s左右。

### 写测试：

```
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.1.3-tests.jar TestDFSIO -write -nrFiles 10 -size 100MB
```

结果：

```
2022-06-19 13:38:30,811 INFO fs.TestDFSIO:         Number of files: 10(文件个数)
2022-06-19 13:38:30,811 INFO fs.TestDFSIO:  Total MBytes processed: 1000（总共写入1000MB）
2022-06-19 13:38:30,811 INFO fs.TestDFSIO:       Throughput mb/sec: 1.42（总写入量/总时间）
2022-06-19 13:38:30,811 INFO fs.TestDFSIO:  Average IO rate mb/sec: 1.54（各节点的平均速度的平均值）
2022-06-19 13:38:30,811 INFO fs.TestDFSIO:   IO rate std deviation: 0.56（各节点的平均速度的方差）
```

由于有10个文件，每个文件有2个副本需要网络传输（有1个在本地），因此实际网络传输速度为：1.42*20=28.4,与实际带宽接近。说明本地IO速度比网络速度明显要快（这里是因为我的电脑是固态硬盘），**网络速度才是瓶颈**。

### 读测试：

```
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-3.1.3-tests.jar TestDFSIO -read -nrFiles 10 -size 100MB
```

结果：

```
2022-06-19 14:00:59,453 INFO fs.TestDFSIO:         Number of files: 10（总共读取10个文件）
2022-06-19 14:00:59,453 INFO fs.TestDFSIO:  Total MBytes processed: 1000（总共读取1000MB文件）
2022-06-19 14:00:59,453 INFO fs.TestDFSIO:       Throughput mb/sec: 36.53（读取量/读取时间）
2022-06-19 14:00:59,453 INFO fs.TestDFSIO:  Average IO rate mb/sec: 78.92（各节点的平均速度的平均值）
2022-06-19 14:00:59,453 INFO fs.TestDFSIO:   IO rate std deviation: 132.65（各节点的平均速度的方差）
```

可以看到读速度远远快于写速度，也快于网络带宽，这是由于10个文件都在本地有副本，无需网络IO。

## Kafka测试

Kafka压测时，可以查看到哪个地方出现了瓶颈（CPU，内存，网络IO）。一般都是网络IO达到瓶颈。  

创建主题：

```
$KAFKA_HOME/bin/kafka-topics.sh --zookeeper hadoop102:2181,hadoop103:2181,hadoop104:2181/kafka  --create --replication-factor 1 --partitions 1 --topic topic_log
```

### 生产测试：

```
$KAFKA_HOME/bin/kafka-producer-perf-test.sh  --topic test --record-size 100 --num-records 100000 --throughput -1 --producer-props bootstrap.servers=hadoop102:9092,hadoop103:9092,hadoop104:9092
record-size是一条信息有多大，单位是字节。
num-records是总共发送多少条信息。
throughput 是每秒多少条信息，设成-1，表示不限流，可测出生产者最大吞吐量。
```

结果：

```
520988 records sent, 104093.5 records/sec (9.93 MB/sec), 1765.0 ms avg latency, 2811.0 ms max latency.
549228 records sent, 109538.9 records/sec (10.45 MB/sec), 2754.9 ms avg latency, 2818.0 ms max latency.
...(省略)
5000000 records sent, 108934.835182 records/sec (10.39 MB/sec), 2662.82 ms avg latency, 2892.00 ms max latency, 2785 ms 50th, 2809 ms 95th, 2840 ms 99th, 2890 ms 99.9th.
```

3个副本，因此网络传输速度为10.39*3=31.117，与实际带宽接近，说明网络传输是瓶颈。

### 消费测试：

```
$KAFKA_HOME/bin/kafka-consumer-perf-test.sh --broker-list hadoop102:9092,hadoop103:9092,hadoop104:9092 --topic topic_log --fetch-size 10000 --messages 10000000 --threads 1 
```

结果：

```
start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
	2022-06-19 21:52:06:893, 2022-06-19 21:53:46:779, 953.6862, 9.5477, 10000125, 100115.3815, 1655646727127, -1655646627241, -0.0000, -0.0060
可见消费速度是9.5477MB/s
```

### kafka分区计算

1）创建一个只有1个分区的topic

2）测试这个topic的producer吞吐量和consumer吞吐量。

3）假设他们的值分别是Tp和Tc，单位可以是MB/s。

4）然后假设总的目标吞吐量是Tt，那么分区数=Tt / min（Tp，Tc）。

从前面的测试可知，生产速度是10.39MB/s，消费速度是9.5477MB/s。本次项目期望吞吐量是20M/s，那么就应该有20/9.5=2个分区
