# 项目介绍

## 业务概述

电商的业务流程可以以一个普通用户的浏览足迹为例进行说明，用户点开电商首页开始浏览，可能会通过分类查询也可能通过全文搜索寻找自己中意的商品，这些商品无疑都是存储在后台的管理系统中的。

当用户寻找到自己中意的商品，可能会想要购买，将商品添加到购物车后发现需要登录，登录后对商品进行结算，这时候购物车的管理和商品订单信息的生成都会对业务数据库产生影响，会生成相应的订单数据和支付数据。

订单正式生成之后，还会对订单进行跟踪处理，直到订单全部完成。

电商的主要业务流程包括用户前台浏览商品时的商品详情的管理，用户商品加入购物车进行支付时用户个人中心&支付服务的管理，用户支付完成后订单后台服务的管理，这些流程涉及到了十几个甚至几十个业务数据表，甚至更多。

以下为本电商数仓系统涉及到的业务数据表结构关系。这34个表以订单表、用户表、SKU商品表、活动表和优惠券表为中心，延伸出了优惠券领用表、支付流水表、活动订单表、订单详情表、订单状态表、商品评论表、编码字典表退单表、SPU商品表等，用户表提供用户的详细信息，支付流水表提供该订单的支付详情，订单详情表提供订单的商品数量等情况，商品表给订单详情表提供商品的详细信息。  

![1655879322355](https://user-images.githubusercontent.com/103411715/175755335-91d82551-9aaf-4db0-aa22-0abc16de3a9e.png)

## 项目需求

1、用户行为数据采集平台搭建

2、业务数据采集平台搭建

3、数据仓库维度建模

4、分析,设备、会员、商品、地区、活动等电商核心主题,统计的报表指标近100个。完全对比中型公司。

5、采用即席查询工具,随时进行指标分析

6、对集群性能进行监控,发生异常需要报警。

## 技术选型

技术选型主要考虑因素:数据量大小、业务需求、行业内经验、技术成熟度、开发维护成本、总成本预算

数据采集传输:Flume,Kafka,Sqoop

数据存储:MySql,HDFS,HBase

数据计算:Hive

数据查询:Kylin

任务调度:Azkaban

## 技术流程

 ![数仓流程图](https://user-images.githubusercontent.com/103411715/174213438-3502163c-ba0c-429d-a15b-c0ccc717575d.png) 

 不直接用flume而是用flume-kafka-flume的原因：1.解耦。只用flume的话数据就只能传到特定的地点（例如hdfs），但是用kafka后数据比较容易被多方使用。2.异步。近期的数据一般比较重要，然kafka作为临时存储，等待使用时可以直接获取。 

## 集群规模说明

假设：

(1)每天日活跃用户100万,每人一天平均100条:100万*100条=1亿条

(2)每条日志1K左右,每天1亿条:100000000/1024/1024=约100G 

(3)半年内不广容服务器来算:100G*180天=约18T 

(4)保存3副本:18T*3=54T 

  (5)预留20%-30%Bf=54T/0.7=77T 

(6)算到这:约10台服务器（每台8T磁盘128G内存）

上述是集群规模的理论选择。**事实上，本次项目只是用三台虚拟机，每台内存2G，磁盘40G,4核。**

# 集群准备

## Hadoop测试

### 写测试

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

由于有10个文件，每个文件有2个副本需要网络传输（有1个在本地），因此实际网络传输速度为：1.42*20=28.4,与实际带宽（300Mps）略低。

### 读测试

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

可以看到读速度远远快于写速度，也快于网络带宽（300Mps），这是由于10个文件都在本地有副本，无需网络IO。

## Kafka测试

### 配置说明

3台设备，每台设备2G内存，4核，100Mps，理论网速峰值为12MB/s。每台kafka分配jvm内存1G，G1回收器。

### Producer压测

测试对象：batch size（批次容量），linger.ms（等待时间），ack（应答方式，0为leader收到消息未落盘时应答，1为leader收到消息且落盘后应答，-1为全部ISR成员落盘成功后应答），partition（分区数），replication（副本数量）。

#### 1.batch size&linger.ms

总数据量5,000,000，单条数据100B，ack=0，repartition=1,replication=1

![1655820259855](https://user-images.githubusercontent.com/103411715/175755389-23f65dc0-f56e-4216-95ce-6f876677e269.png)

结论：**batch size为64K，linger.ms=20ms性能最佳。**这可能是batch size较小时在linger.ms之前就装满了，因此增加linger.ms不会带来明显影响。而batch size达到64K时每次发送的量比较多，发送次数比较少，此时增加等待时长能有效地提高生产速度。

#### 2.ack

总数据量5,000,000，单条数据100B，repartition=1，batch size=64K,linger.ms=20ms.,replication=1

![1655821127852](https://user-images.githubusercontent.com/103411715/175755399-5c1aa5ed-c9b5-43ff-99b0-7da27ee3bc2b.png)

leader落盘前应答（ack=0)和leader落盘后应答(ack=1)的生产速度几乎没有区别，但是ISR成员全部落盘后应答的生产速度只有前者的38%，性能严重下降。**如果数据可以接受部分丢失，不要设置ack=-1**。

#### 3.partition

总数据量5,000,000，单条数据100B，ack=0,，batch size=64K,linger.ms=20ms,replication=1

![1655869609636](https://user-images.githubusercontent.com/103411715/175755403-892723f3-553b-4e74-a434-36ee11125076.png)

 结论：分区数越多，单线程消费者吞吐率越小。随着更多的broker线程和磁盘开始扫描不同的分区，吞吐量开始显著增加。但是，**一旦使用了所有broker线程和磁盘，添加额外的分区没有任何效果。** 因为每台虚拟机都是4线程，因此在4分区或5分区时性能最佳。

不过，partition建议不要太少，因为partition数量很少的情况下出现数据倾斜的可能性也比较大，木桶效应会使得集群性能急剧下降。因此这里建议用12分区。

#### 4.replication

总数据量5,000,000，单条数据100B，ack=0,partition=12,batch size=64K,linger.ms=20ms

![1655869609636](https://user-images.githubusercontent.com/103411715/175755413-30d3bcc3-9573-428f-a935-2bfc761a9e5d.png)

 结论：**备份数越多，吞吐率越低。** 

### Consumer压测

测试对象：fetch size(每次抓取数据量)，fetch threads(抓取数据的线程的数量)

#### 1.fetch size

总数据量5,000,000，单条数据100B,fetch threads=1

![1655869609636](https://user-images.githubusercontent.com/103411715/175755427-13eed69d-bf74-4516-9d8c-8ddc6c50539b.png)

fetch size从100K提升至400K的过程中，消费速度**小幅度**提升。

#### 2.fetch threads

总数据量5,000,000，单条数据100B,fetch size=400K

![1655823788723](https://user-images.githubusercontent.com/103411715/175755437-91f5348a-5c2d-463c-8ef0-426f1882ccce.png)

fetch thread的数量并不能产生明显影响，在fetch thread=12时消费速度略微高于其他情况。

### 容灾测试

#### 1.集群宕机

即时设置了ack=-1（ISR成员全部落盘后应答），也不能保证数据不丢失。生产者在重发多次失败后会将数据丢弃。

#### 2.部分节点宕机

只要宕机节点数少于（副本数-1），在宕机后后自动Rebalance后生产和消费正常进行。但是，rebalance会带来明显的性能开销，整个集群的性能会在一段时间内明显下降。

#### 3.磁盘故障

磁盘故障会导致所有leader在该broker的分区无法被访问，**却不会触发rebalance，导致问题一直不能被解决，建议立即手动下线该broker**。
# kafka监控和调优

关注：

1.网络线程空余量（network processor idle和 request handler idle ）是否超过30%，如果低于30%，说明整个集群的压力过大，可能无法应对流量高峰的冲击。

2.shrink指标。如果副本频频失联，会导致频繁的rebalance。

3.各个broker的等待队列（request queue size）数量。如果接近默认值500，说明broker的请求压力很大。

4.各个topic的生产量和消费量。

措施：

1. 适当减少写压力很大的topic 的batch size或减少linger.ms，虽然增加了延迟但是能提升写入性能。如果topic的每条消息都很长，应当适当增加缓冲池的大小，减少缓冲池阻塞的出现。
2. 如果topic的消费压力很大，可以考虑增加分区。

3. 如果副本频频失联，先检查设备问题（节点是否挂掉，网络是否稳定），在保证设备无显著问题的情况后仍然有问题，适当增加超时时间，或减少心跳间隔，或增加每次消息的处理时间，或减少每次处理的消息量。

##  flume测试

单台flume，jvm内存1G。source均为taildir source。

![1655870830054](https://user-images.githubusercontent.com/103411715/175755452-7ff015ba-d419-4483-9c13-ee4acebf0312.png)

可见，采集速度从慢到快依次为：

1.taildir source+file channel+kafka sink

2.taildir source+file channel+kafka sink

3.taildir source+file channel+hdfs sink

4.taildir source+memory channel+kafka sink

5.taildir source+memory channel+file sink

6.taildir source+kafka channel

可见，相较memory channel，file channel会使得采集性能急剧下降。如果想要保证数据不丢失，建议使用kafka channel。

# 数仓搭建

## 数据采集

### 日志数据采集

日志采集的flume架构如下：

![未命名文件 (2)](https://user-images.githubusercontent.com/103411715/175755471-98854fe5-dc79-4748-b9d7-73f84a64e064.png)

拦截器：抽取日志中的时间戳设置为header，后面hdfs sink会根据该时间戳来讲数据放入相应的文件夹中。

Kafka channel：数据保存在kafka中，方便其他人使用。

通过taildir source将指定路径下的日志采集到kafka channel中，代码如下：

```
#为各组件命名
a1.sources = r1
a1.channels = c1

#描述source
a1.sources.r1.type = TAILDIR
a1.sources.r1.filegroups = f1
a1.sources.r1.filegroups.f1 = /opt/module/applog/log/app.*
a1.sources.r1.positionFile = /opt/module/flume/taildir_position.json

#描述channel
a1.channels.c1.type = org.apache.flume.channel.kafka.KafkaChannel
a1.channels.c1.kafka.bootstrap.servers = hadoop102:9092,hadoop103:9092
a1.channels.c1.kafka.topic = topic_log
a1.channels.c1.parseAsFlumeEvent = false

#绑定source和channel以及sink和channel的关系
a1.sources.r1.channels = c1
```

通过kafka source将数据从kafka中采集到hdfs上，为了让hdfs sink能够使用日志数据中的时间戳，通过拦截器增加header，名为"timestamp"，代码如下：

```
## 组件
a1.sources=r1
a1.channels=c1
a1.sinks=k1

## source1
a1.sources.r1.type = org.apache.flume.source.kafka.KafkaSource
a1.sources.r1.batchSize = 5000
a1.sources.r1.batchDurationMillis = 2000
a1.sources.r1.kafka.bootstrap.servers = hadoop102:9092,hadoop103:9092,hadoop104:9092
a1.sources.r1.kafka.topics=topic_log
a1.sources.r1.interceptors = i1
a1.sources.r1.interceptors.i1.type = flume.interceptor.TimeStampInterceptor$Builder

## channel1
a1.channels.c1.type = file
a1.channels.c1.checkpointDir = /opt/module/flume/checkpoint/behavior1
a1.channels.c1.dataDirs = /opt/module/flume/data/behavior1/
a1.channels.c1.maxFileSize = 2146435071
a1.channels.c1.capacity = 1000000
a1.channels.c1.keep-alive = 6

## sink1
a1.sinks.k1.type = hdfs
a1.sinks.k1.hdfs.path = /origin_data/gmall/log/topic_log/%Y-%m-%d
a1.sinks.k1.hdfs.filePrefix = log-
a1.sinks.k1.hdfs.round = false

a1.sinks.k1.hdfs.rollInterval = 10
a1.sinks.k1.hdfs.rollSize = 134217728
a1.sinks.k1.hdfs.rollCount = 0

## 控制输出文件是原生文件。
a1.sinks.k1.hdfs.fileType = CompressedStream
a1.sinks.k1.hdfs.codeC = lzop

## 拼装
a1.sources.r1.channels = c1
a1.sinks.k1.channel= c1
```

拦截器核心代码如下：

```java
@Override
public Event intercept(Event event) {
    Map<String, String> headers = event.getHeaders();
    String log = new String(event.getBody(), StandardCharsets.UTF_8);
    JSONObject jsonObject = JSON.parseObject(log);//利用fastjson解析日志
    String ts = jsonObject.getString("ts");//从日志抽取时间戳
    headers.put("timestamp", ts);//设置header
    return event;
}
```

### 业务数据采集

业务数据通过sqoop将数据从mysql同步到hdfs中。

**1.同步策略**

全量同步：每日全量,就是每天存储一份完整数据,作为一个分区。适用于表数据量不大,且每天既会有新数据插入,也会有旧数据的修改的场景。例如:商品分类表、活动表、优惠券表等。

增量同步：每日增量,就是每天存储一份增量数据,作为一个分区。适用于表数据量大,且每天只会有新数据插入的场景。例如:订单详情表，用户评论表，支付流水表，退单表等。

新增及变化：每日新增及变化,就是存储创建时间和操作时间都是今天的数据。适用场景为,表的数据量大,既会有新增,又会有变化。例如:用户表、订单表、优惠卷领用表。

特殊策略：不会发生变化的客观的数据，例如：性别、日期、尺码、国家地区等。

**2.sqoop采集命令示例**

这是商品分类表，同步策略是全量同步，因此全部采集没有采集条件。

```
QUERY="select 
    id,
    spu_id,
    price,
    sku_name,
    sku_desc,
    weight,
    tm_id,
    category3_id,
    create_time
    from sku_info"
sqoop import \
--connect jdbc:mysql://hadoop102:3306/APP \
--username root \
--password root \
--target-dir /origin_data/APP/db//$do_date \
--delete-target-dir \
--query "$QUERY" \
--num-mappers 1 \
--fields-terminated-by '\t' \
--compress \
--compression-codec lzop \
--null-string '\\N' \
--null-non-string '\\N'
```

这是订单详情表的采集命令，同步策略是增量更新，因此sql语句中筛选了日期为当天的数据。

```
QUERY=""select 
            od.id,
            order_id, 
            user_id, 
            sku_id,
            sku_name,
            order_price,
            sku_num, 
            od.create_time,
            source_type,
            source_id  
            from order_detail od
            join order_info oi
            on od.order_id=oi.id
            where DATE_FORMAT(od.create_time,'%Y-%m-%d')='$do_date'
"
sqoop import \
--connect jdbc:mysql://hadoop102:3306/APP \
--username root \
--password root \
--target-dir /origin_data/APP/db//$do_date \
--delete-target-dir \
--query "$QUERY" \
--num-mappers 1 \
--fields-terminated-by '\t' \
--compress \
--compression-codec lzop \
--null-string '\\N' \
--null-non-string '\\N'
```

这是用户表，同步策略是新增及变化，因此sql语句中筛选了当天创建的用户数据或者当前发生修改的用户数据。

```
QUERY="select 
            id,
            name,
            birthday,
            gender,
            email,
            user_level, 
            create_time,
            operate_time
            from user_info 
            where (DATE_FORMAT(create_time,'%Y-%m-%d')='$do_date' 
            or DATE_FORMAT(operate_time,'%Y-%m-%d')='$do_date')
"
sqoop import \
--connect jdbc:mysql://hadoop102:3306/APP \
--username root \
--password root \
--target-dir /origin_data/APP/db//$do_date \
--delete-target-dir \
--query "$QUERY" \
--num-mappers 1 \
--fields-terminated-by '\t' \
--compress \
--compression-codec lzop \
--null-string '\\N' \
--null-non-string '\\N'
```

## 数仓分层

ODS层:原始数据层,存放原始数据,直接加载原始日志、数据,数据保持原貌不做处理。flume-kafka-flume架构采集到hdfs的日志数据和mysql通过sqoop传到hdfs的业务数据，都属于这一层。

DWD层:对ODS层数据进行清洗(去除空值,脏数据,超过极限范围的数据)、脱敏等。保存明细数据,一行信息代表一次业务行为,例如一次下单。

DWS层：以DWD为基础,按天进行轻度汇总。一行信息代表一个主题对象一天的汇总行为,例如一个用户一天下单次数.

DWT层：以DWS为基础,对数据进行累积汇总。一行信息代表一个主题对象的累积行为,例如一个用户从注册那天开始至今一共下了多少次单.

ADS层：为各种统计报表提供数据。

**分层的好处**：1.把复杂问题简单化。2)减少重复开发。3.隔离原始数据。

## 维度建模

当今的数据处理大致可以分成两大类：联机事务处理OLTP（on-line transaction processing）、联机分析处理OLAP（On-Line Analytical Processing）。   OLTP主要是对数据的增删改，侧重实时性，OLAP是对数据的查询，侧重大数据量查询。

维度模型主要应用于OLAP系统中，通常以某一个事实表为中心进行表的组织，主要面向业务，特征是可能存在数据的冗余，但是能方便的得到数据。关系模型虽然冗余少，但是在大规模数据，跨表分析统计查询过程中，会造成多表关联，这会大大降低执行效率。所以这里采用维度模型建模，选择星型模型+星座模型，因为hive中的join意味着shuffle，性能开销很大，用星型模型可以减少查询时的join操作。

## 数仓搭建-ODS层  

引擎：hive on spark。日志数据通过flume采集到hdfs，业务数据从mysql通过sqoop采集到hdfs。ODS层的任务是：

建表，数据加载到表中。所有日志数据建立一张表，按天分区，数据不做任何提取，因此只有一列。mysql每张业务表都会在hive建立对应的表，结构完全相同，按天分区。

日志表建表示例：

```sql
drop table if exists ods_log;
CREATE EXTERNAL TABLE ods_log (`line` string)
PARTITIONED BY (`dt` string) -- 按照时间创建分区
STORED AS -- 指定存储方式，读数据采用LzoTextInputFormat；
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/warehouse/gmall/ods/ods_log'  -- 指定数据在hdfs上的存储位置
;
```

业务表建表示例： 

```sql
rop table if exists ods_order_info;
create external table ods_order_info (
    `id` string COMMENT '订单号',
    `final_total_amount` decimal(16,2) COMMENT '订单金额',
    `order_status` string COMMENT '订单状态',
    `user_id` string COMMENT '用户id',
    `out_trade_no` string COMMENT '支付流水号',
    `create_time` string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间',
    `province_id` string COMMENT '省份ID',
    `benefit_reduce_amount` decimal(16,2) COMMENT '优惠金额',
    `original_total_amount` decimal(16,2)  COMMENT '原价金额',
    `feight_fee` decimal(16,2)  COMMENT '运费'
) COMMENT '订单表'
PARTITIONED BY (`dt` string) -- 按照时间创建分区
row format delimited fields terminated by '\t' -- 指定分割符为\t 
STORED AS -- 指定存储方式，读数据采用LzoTextInputFormat；输出数据采用TextOutputFormat
  INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_order_info/' -- 指定数据在hdfs上的存储位置
;
```

数据加载示例

```
load data inpath '/origin_data/gmall/log/topic_log/2020-06-14' into table ods_log partition(dt='2020-06-14');
```

## 数仓搭建-DWD层  

本层内容：1）对用户行为数据解析。2）对核心数据进行判空过滤。3）对业务数据采用**维度模型**重新建模。

### 数据格式

日志数据分为普通日志和启动日志。普通日志包含common（用户信息）+actions（用户动作，例如添加购物车）+display（页面具体模块，例如查询模块）+page（页面信息）.

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

启动日志包含common（用户信息）+start(启动信息)+错误信息+时间戳。

```
{
  "common": {
    "ar": "370000",
    "ba": "Honor",
    "ch": "wandoujia",
    "md": "Honor 20s",
    "mid": "eQF5boERMJFOujcp",
    "os": "Android 11.0",
    "uid": "76",
    "vc": "v2.1.134"
  },
  "start": {   
    "entry": "icon",         --icon手机图标  notice 通知   install 安装后启动
    "loading_time": 18803,  --启动加载时间
    "open_ad_id": 7,        --广告页ID
    "open_ad_ms": 3449,    -- 广告总共播放时间
    "open_ad_skip_ms": 1989   --  用户跳过广告时点
  },
"err":{                     --错误
"error_code": "1234",      --错误码
    "msg": "***********"       --错误信息
},
  "ts": 1585744304000
}
```

要将日志进行分类，并提取其中的信息为不同列，需要借助hive函数get_json_object()。该函数需要传入两个参数，第一个参数是字符串，可以是json字符串也可以是jsonarray字符串，第二个参数表示你要提取的部分。例如'$[0]'表示jsonarray的第一个json object。如果是json object，可以用'$.属性名'来获取其中某个属性对应的值。

日志分成这几种表：启动日志表、页面日志表、动作日志表、曝光日志表、错误日志表。下面以启动日志表为例，其他表也基本相同。

```sql
drop table if exists dwd_start_log;
CREATE EXTERNAL TABLE dwd_start_log(
    `area_code` string COMMENT '地区编码',
    `brand` string COMMENT '手机品牌', 
    `channel` string COMMENT '渠道', 
    `model` string COMMENT '手机型号', 
    `mid_id` string COMMENT '设备id', 
    `os` string COMMENT '操作系统', 
    `user_id` string COMMENT '会员id', 
    `version_code` string COMMENT 'app版本号', 
    `entry` string COMMENT ' icon手机图标  notice 通知   install 安装后启动',
    `loading_time` bigint COMMENT '启动加载时间',
    `open_ad_id` string COMMENT '广告页ID ',
    `open_ad_ms` bigint COMMENT '广告总共播放时间', 
    `open_ad_skip_ms` bigint COMMENT '用户跳过广告时点', 
    `ts` bigint COMMENT '时间'
) COMMENT '启动日志表'
PARTITIONED BY (dt string) -- 按照时间创建分区
stored as parquet -- 采用parquet列式存储
LOCATION '/warehouse/gmall/dwd/dwd_start_log' -- 指定在HDFS上存储位置
TBLPROPERTIES('parquet.compression'='lzo') -- 采用LZO压缩
;

```



```sql
insert overwrite table dwd_start_log partition(dt='2020-06-10')
select 
    get_json_object(line,'$.common.ar'),
    get_json_object(line,'$.common.ba'),
    get_json_object(line,'$.common.ch'),
    get_json_object(line,'$.common.md'),
    get_json_object(line,'$.common.mid'),
    get_json_object(line,'$.common.os'),
    get_json_object(line,'$.common.uid'),
    get_json_object(line,'$.common.vc'),
    get_json_object(line,'$.start.entry'),
    get_json_object(line,'$.start.loading_time'),
    get_json_object(line,'$.start.open_ad_id'),
    get_json_object(line,'$.start.open_ad_ms'),
    get_json_object(line,'$.start.open_ad_skip_ms'),
    get_json_object(line,'$.ts')
from ods_log
where dt='2020-06-10'
and get_json_object(line,'$.start') is not null;

```

### 业务数据处理

#### 业务表的分类

业务表根据数据的特点可以分为：事实表和维度表两种。

**1.维度表**

一般是对事实的**描述信息**。每一张维表对应现实世界中的一个对象或者概念。  例如：用户、商品、日期、地区等。

**维度表的特征：**

1.维度表的范围很宽（具有多个属性、列比较多）

2.跟事实表相比，行数相对较小：通常< 10万条

3.内容相对固定：编码表

**2.事实表**

事实表中的每行数据代表一个业务事件（下单、支付、退款、评价等）。

每一个事实表的行包括：具有可加性的数值型的度量值、与维表相连接的外键，通常具有两个和两个以上的外键。

**事实表的特征：**

1.非常的大

2.内容相对的窄：列数较少（主要是外键id和度量值）

3.经常发生变化，每天会新增加很多。

事实表可以分为：

1**）事务型事实表**

以**每个事务或事件为单位**，例如一个销售订单记录，一笔支付记录等，作为事实表里的一行数据。一旦事务被提交，事实表数据被插入，数据就不再进行更改。支付事实表 、退款事实表、评价事实表、订单明细事实表都是事务型事实表。

2**）周期型快照事实表**

周期型快照事实表中**不会保留所有数据**，**只保留固定时间间隔的数据**，例如每天或者每月的销售额，或每月的账户余额等。例如购物车，有加减商品，随时都有可能变化，但是我们更关心每天结束时这里面有多少商品，方便我们后期统计分析。加购事实表、收藏事实表都是周期型快照事实表。

3**）累积型快照事实表**

**累计快照事实表用于跟踪业务事实的变化。**例如，数据仓库中可能需要累积或者存储订单从下订单开始，到订单商品被打包、运输、和签收的各个业务阶段的时间点数据来跟踪订单声明周期的进展情况。当这个业务过程进行时，事实表的记录也要不断更新。优惠券领用事实表、订单事实表都是累计型快照事实表。

#### 具体处理

![1655905394007](https://user-images.githubusercontent.com/103411715/175755535-8891bea7-f589-447c-8bda-37966d42ce8d.png)

维度表相较事实表要少很多，一般是用全量同步策略。用户维度表的数据比较多，并且还需要关注历史信息，不适合使用新增及变化同步策略，适合用拉链表。

DWD层的数据来自于ODS层，DWD有自己的同步策略，数据来源的表又有自己的同步策略。根据数据来源的情况可以分为种情况：

1.数据来源全部是全量同步策略：数据量不大维度表和周期型快照事实表。

这种情况只需要把数据来源当天的数据join到一起，insert到宽表当天的分区就可以（因为当天的数据就已经是最新全量数据了）。

2.数据来源至少有一部分是增量同步策略，其他的都是全量同步策略：事务型事实表。

这种情况只需要把数据来源当天的数据join到一起，insert到宽表当天的分区就可以（因为当天的数据就已经是增量数据了）。

3.数据来源至少有一部分是新增及变化策略，宽表只保留最新数据：累计型快照事实表

这种情况比较复杂，举例说明。例如优惠券领用事实表，描述的是用户领用优惠券的情况：领用，使用，支付，假设全部数据如下（当前日期是2022/06/03):

| 优惠券id | 领用时间（分区列） | 使用时间 | 支付时间 | 维度表诸多外键 |
| -------- | ------------------ | -------- | -------- | -------------- |
| 1        | 6-01               | null     | null     | *              |
| 2        | 6-01               | 6-02     | null     | *              |
| 3        | 6-02               | null     | null     | *              |

然后今天（6-03）优惠券的领用情况是：

| 优惠券id | 领用时间 | 使用时间 | 支付时间 |
| -------- | -------- | -------- | -------- |
| 1        | 6-01     | 6-03     |          |
| 2        | 6-01     | 6-02     | 6-03     |
| 4        | 6-03     | null     | null     |

那么今天更新的方式是：将原来的优惠券领用表和今天的优惠券领用情况进行join，然后如果出现了对旧数据的修改，则使用新数据，否则使用旧数据。示例代码如下：

```sql
insert overwrite table dwd_fact_coupon_use partition(dt)
select --full join后如果右边不为空说明该信息是今天出现的，取右边
    if(new.id is null,old.id,new.id),
    if(new.coupon_id is null,old.coupon_id,new.coupon_id),
    if(new.user_id is null,old.user_id,new.user_id),
    if(new.order_id is null,old.order_id,new.order_id),
    if(new.coupon_status is null,old.coupon_status,new.coupon_status),
    if(new.get_time is null,old.get_time,new.get_time),
    if(new.using_time is null,old.using_time,new.using_time),
    if(new.used_time is null,old.used_time,new.used_time),
    date_format(if(new.get_time is null,old.get_time,new.get_time),'yyyy-MM-dd')
from(
    select
        id,
        coupon_id,
        user_id,
        order_id,
        coupon_status,
        get_time,
        using_time,
        used_time
    from dwd_fact_coupon_use --这是旧的优惠券领用事实表
    where dt in(
        select
            date_format(get_time,'yyyy-MM-dd')
        from ods_coupon_use
        where dt='2020-06-14'
    )
)old
full outer join
(
    select
        id,
        coupon_id,
        user_id,
        order_id,
        coupon_status,
        get_time,
        using_time,
        used_time
    from ods_coupon_use --这是今天的优惠券领用情况
    where dt='2020-06-14'
)new
on old.id=new.id;


```

4.数据来源至少有一部分是新增及变化策略，宽表保留最新数据和历史数据：拉链表（这里是用户维度表）

拉链表,记录每条信息的生命周期,一旦一条记录的生命周期结束,就重新开始一条新的记录,并把当前日期放入生效开始日期。如果当前信息至今有效,在生效结束日期中填入一个极大值(如9999-9999)。

拉链表适合于:数据会发生变化,但是大部分是不变的。(即:缓慢变化维)》比如:用户信息会发生变化,但是每天变化的比例不高。如果数据量有一定规模,按照每日全量的方式保存效率很低。比如:1亿用户365天,每天一份用户信息。(做每日全量效率低)。

通过,生效开始日期<=某个日期且生效结束日期=某个日期,能够得到某个时间点的数据全量切片。

```sql
insert overwrite table dwd_dim_user_info_his_tmp
select *
from(
    select 
        id,
        name,
        birthday,
        gender,
        email,
        user_level,
        create_time,
        operate_time,
        '2020-06-15' start_date,
        '9999-99-99' end_date
    from ods_user_info where dt='2020-06-15'

union all 

    select 
        uh.id,
        uh.name,
        uh.birthday,
        uh.gender,
        uh.email,
        uh.user_level,
        uh.create_time,
        uh.operate_time,
        uh.start_date,
        if(ui.id is not null  and uh.end_date='9999-99-99', date_add(ui.dt,-1), uh.end_date) end_date
from dwd_dim_user_info_his uh 
left join(
        select
            *
        from ods_user_info
        where dt='2020-06-15'
) ui 
on uh.id=ui.id
)his 


```

## 数仓搭建-DWS层

### 业务术语

1）用户
用户以设备为判断标准，在移动统计中，每个独立设备认为是一个独立用户。Android系统根据IMEI号，IOS系统根据OpenUDID来标识一个独立用户，每部手机一个用户。
2）新增用户
首次联网使用应用的用户。如果一个用户首次打开某APP，那这个用户定义为新增用户；卸载再安装的设备，不会被算作一次新增。新增用户包括日新增用户、周新增用户、月新增用户。
3）活跃用户
打开应用的用户即为活跃用户，不考虑用户的使用情况。每天一台设备打开多次会被计为一个活跃用户。
4）周（月）活跃用户
某个自然周（月）内启动过应用的用户，该周（月）内的多次启动只记一个活跃用户。
5）月活跃率
月活跃用户与截止到该月累计的用户总和之间的比例。
6）沉默用户
用户仅在安装当天（次日）启动一次，后续时间无再启动行为。该指标可以反映新增用户质量和用户与APP的匹配程度。
7）版本分布
不同版本的周内各天新增用户数，活跃用户数和启动次数。利于判断APP各个版本之间的优劣和用户行为习惯。
8）本周回流用户
上周未启动过应用，本周启动了应用的用户。
9）连续n周活跃用户
连续n周，每周至少启动一次。
10）忠诚用户
连续活跃5周以上的用户
11）连续活跃用户
连续2周及以上活跃的用户
12）近期流失用户
连续n（2<= n <= 4）周没有启动应用的用户。（第n+1周没有启动过）
13）留存用户
某段时间内的新增用户，经过一段时间后，仍然使用应用的被认作是留存用户；这部分用户占当时新增用户的比例即是留存率。
例如，5月份新增用户200，这200人在6月份启动过应用的有100人，7月份启动过应用的有80人，8月份启动过应用的有50人；则5月份新增用户一个月后的留存率是50%，二个月后的留存率是40%，三个月后的留存率是25%。
14）用户新鲜度
每天启动应用的新老用户比例，即新增用户数占活跃用户数的比例。
15）单次使用时长
每次启动使用的时间长度。
16）日使用时长
累计一天内的使用时间长度。
17）启动次数计算标准
IOS平台应用退到后台就算一次独立的启动；Android平台我们规定，两次启动之间的间隔小于30秒，被计算一次启动。用户在使用过程中，若因收发短信或接电话等退出应用30秒又再次返回应用中，那这两次行为应该是延续而非独立的，所以可以被算作一次使用行为，即一次启动。业内大多使用30秒这个标准，但用户还是可以自定义此时间间隔。

### 每日设备行为

设备的唯一标识是：mid_id(设备id),brand(手机品牌),model(手机型号)。每日设备行为表统计的是昨天设备启动次数和访问的所有页面。

建表：

```sql
create external table dws_uv_detail_daycount
(
    `mid_id`      string COMMENT '设备id',
    `brand`       string COMMENT '手机品牌',
    `model`       string COMMENT '手机型号',
    `login_count` bigint COMMENT '活跃次数',
    `page_stats`  array<struct<page_id:string,page_count:bigint>> COMMENT '页面访问统计'
) COMMENT '每日设备行为表'
partitioned by(dt string)
stored as parquet
location '/warehouse/gmall/dws/dws_uv_detail_daycount'
tblproperties ("parquet.compression"="lzo");

```

数据加载：

```sql
with
tmp_start as
(
    select  
        mid_id,
        brand,
        model,
        count(*) login_count
    from dwd_start_log
    where dt='2020-06-14'
    group by mid_id,brand,model
),
tmp_page as
(
    select
        mid_id,
        brand,
        model,
    	collect_set(named_struct('page_id',page_id,'page_count',page_count)) page_stats
    from
    (
        select
            mid_id,
            brand,
            model,
            page_id,
            count(*) page_count
        from dwd_page_log
        where dt='2020-06-14'
        group by mid_id,brand,model,page_id
    )tmp
    group by mid_id,brand,model
)
insert overwrite table dws_uv_detail_daycount partition(dt='2020-06-14')
select
    nvl(tmp_start.mid_id,tmp_page.mid_id),
    nvl(tmp_start.brand,tmp_page.brand),
    nvl(tmp_start.model,tmp_page.model),
    tmp_start.login_count,
    tmp_page.page_stats
from tmp_start 
full outer join tmp_page
on tmp_start.mid_id=tmp_page.mid_id
and tmp_start.brand=tmp_page.brand
and tmp_start.model=tmp_page.model;

```

### 每日商品行为

每日商品行为表统计的是每日商品被下单数据、支付数据、退款数据、评价数据。

```sql
drop table if exists dws_sku_action_daycount;
create external table dws_sku_action_daycount 
(   
    sku_id string comment 'sku_id',
    order_count bigint comment '被下单次数',
    order_num bigint comment '被下单件数',
    order_amount decimal(16,2) comment '被下单金额',
    payment_count bigint  comment '被支付次数',
    payment_num bigint comment '被支付件数',
    payment_amount decimal(16,2) comment '被支付金额',
    refund_count bigint  comment '被退款次数',
    refund_num bigint comment '被退款件数',
    refund_amount  decimal(16,2) comment '被退款金额',
    cart_count bigint comment '被加入购物车次数',
    favor_count bigint comment '被收藏次数',
    appraise_good_count bigint comment '好评数',
    appraise_mid_count bigint comment '中评数',
    appraise_bad_count bigint comment '差评数',
    appraise_default_count bigint comment '默认评价数'
) COMMENT '每日商品行为'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_sku_action_daycount/'
tblproperties ("parquet.compression"="lzo");

```

### 每日会员行为

每日会员行为表统计的是昨天每个会员登录次数、加入购物车次数、下单次数、下单金额、支付次数、支付金额。

建表：

```sql
create external table dws_user_action_daycount
(   
    user_id string comment '用户 id',
    login_count bigint comment '登录次数',
    cart_count bigint comment '加入购物车次数',
    order_count bigint comment '下单次数',
    order_amount    decimal(16,2)  comment '下单金额',
    payment_count   bigint      comment '支付次数',
    payment_amount  decimal(16,2) comment '支付金额',
    order_detail_stats array<struct<sku_id:string,sku_num:bigint,order_count:bigint,order_amount:decimal(20,2)>> comment '下单明细统计'
) COMMENT '每日会员行为'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_user_action_daycount/'
tblproperties ("parquet.compression"="lzo");
```

数据加载：

```sql
with
tmp_login as
(
    select
        user_id,
        count(*) login_count
    from dwd_start_log
    where dt='2020-06-14'
    and user_id is not null
    group by user_id
),
tmp_cart as
(
    select
        user_id,
        count(*) cart_count
    from dwd_action_log
    where dt='2020-06-14'
    and user_id is not null
    and action_id='cart_add'
    group by user_id
),tmp_order as
(
    select
        user_id,
        count(*) order_count,
        sum(final_total_amount) order_amount
    from dwd_fact_order_info
    where dt='2020-06-14'
    group by user_id
) ,
tmp_payment as
(
    select
        user_id,
        count(*) payment_count,
        sum(payment_amount) payment_amount
    from dwd_fact_payment_info
    where dt='2020-06-14'
    group by user_id
),
tmp_order_detail as
(
    select
        user_id,
        collect_set(named_struct('sku_id',sku_id,'sku_num',sku_num,'order_count',order_count,'order_amount',order_amount)) order_stats
    from
    (
        select
            user_id,
            sku_id,
            sum(sku_num) sku_num,
            count(*) order_count,
            cast(sum(final_amount_d) as decimal(20,2)) order_amount
        from dwd_fact_order_detail
        where dt='2020-06-14'
        group by user_id,sku_id
    )tmp
    group by user_id
)

insert overwrite table dws_user_action_daycount partition(dt='2020-06-14')
select
    tmp_login.user_id,
    login_count,
    nvl(cart_count,0),
    nvl(order_count,0),
    nvl(order_amount,0.0),
    nvl(payment_count,0),
    nvl(payment_amount,0.0),
    order_stats
from tmp_login
left join tmp_cart on tmp_login.user_id=tmp_cart.user_id
left join tmp_order on tmp_login.user_id=tmp_order.user_id
left join tmp_payment on tmp_login.user_id=tmp_payment.user_id
left join tmp_order_detail on tmp_login.user_id=tmp_order_detail.user_id;

```

###  每日活动行为

每日活动行为表统计的是昨天各个活动下参与的下单次数、下单金额、支付次数、支付金额。

```sql
drop table if exists dws_activity_info_daycount;
create external table dws_activity_info_daycount(
    `id` string COMMENT '编号',
    `activity_name` string  COMMENT '活动名称',
    `activity_type` string  COMMENT '活动类型',
    `start_time` string  COMMENT '开始时间',
    `end_time` string  COMMENT '结束时间',
    `create_time` string  COMMENT '创建时间',
    `display_count` bigint COMMENT '曝光次数',
    `order_count` bigint COMMENT '下单次数',
    `order_amount` decimal(20,2) COMMENT '下单金额',
    `payment_count` bigint COMMENT '支付次数',
    `payment_amount` decimal(20,2) COMMENT '支付金额'
) COMMENT '每日活动统计'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_activity_info_daycount/'
tblproperties ("parquet.compression"="lzo");

```

```sql
with
tmp_op as
(
    select
        activity_id,
        sum(if(date_format(create_time,'yyyy-MM-dd')='2020-06-14',1,0)) order_count,
        sum(if(date_format(create_time,'yyyy-MM-dd')='2020-06-14',final_total_amount,0)) order_amount,
        sum(if(date_format(payment_time,'yyyy-MM-dd')='2020-06-14',1,0)) payment_count,
        sum(if(date_format(payment_time,'yyyy-MM-dd')='2020-06-14',final_total_amount,0)) payment_amount
    from dwd_fact_order_info
    where (dt='2020-06-14' or dt=date_add('2020-06-14',-1))
    and activity_id is not null
    group by activity_id
),
tmp_display as
(
    select
        item activity_id,
        count(*) display_count
    from dwd_display_log
    where dt='2020-06-14'
    and item_type='activity_id'
    group by item
),
tmp_activity as
(
    select
        *
    from dwd_dim_activity_info
    where dt='2020-06-14'
)
insert overwrite table dws_activity_info_daycount partition(dt='2020-06-14')
select
    nvl(tmp_op.activity_id,tmp_display.activity_id),
    tmp_activity.activity_name,
    tmp_activity.activity_type,
    tmp_activity.start_time,
    tmp_activity.end_time,
    tmp_activity.create_time,
    tmp_display.display_count,
    tmp_op.order_count,
    tmp_op.order_amount,
    tmp_op.payment_count,
    tmp_op.payment_amount
from tmp_op
full outer join tmp_display on tmp_op.activity_id=tmp_display.activity_id
left join tmp_activity on nvl(tmp_op.activity_id,tmp_display.activity_id)=tmp_activity.id;

```

### 每日地区行为

每日地区行为统计的是昨天各个地区的活跃设备数和支付情况。

```sql
create external table dws_area_stats_daycount(
    `id` bigint COMMENT '编号',
    `province_name` string COMMENT '省份名称',
    `area_code` string COMMENT '地区编码',
    `iso_code` string COMMENT 'iso编码',
    `region_id` string COMMENT '地区ID',
    `region_name` string COMMENT '地区名称',
    `login_count` string COMMENT '活跃设备数',
    `order_count` bigint COMMENT '下单次数',
    `order_amount` decimal(20,2) COMMENT '下单金额',
    `payment_count` bigint COMMENT '支付次数',
    `payment_amount` decimal(20,2) COMMENT '支付金额'
) COMMENT '每日地区统计表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_area_stats_daycount/'
tblproperties ("parquet.compression"="lzo");

```

```sql
with 
tmp_login as
(
    select
        area_code,
        count(*) login_count
    from dwd_start_log
    where dt='2020-06-14'
    group by area_code
),
tmp_op as
(
    select
        province_id,
        sum(if(date_format(create_time,'yyyy-MM-dd')='2020-06-14',1,0)) order_count,
        sum(if(date_format(create_time,'yyyy-MM-dd')='2020-06-14',final_total_amount,0)) order_amount,
        sum(if(date_format(payment_time,'yyyy-MM-dd')='2020-06-14',1,0)) payment_count,
        sum(if(date_format(payment_time,'yyyy-MM-dd')='2020-06-14',final_total_amount,0)) payment_amount
    from dwd_fact_order_info
    where (dt='2020-06-14' or dt=date_add('2020-06-14',-1))
    group by province_id
)
insert overwrite table dws_area_stats_daycount partition(dt='2020-06-14')
select
    pro.id,
    pro.province_name,
    pro.area_code,
    pro.iso_code,
    pro.region_id,
    pro.region_name,
    nvl(tmp_login.login_count,0),
    nvl(tmp_op.order_count,0),
    nvl(tmp_op.order_amount,0.0),
    nvl(tmp_op.payment_count,0),
    nvl(tmp_op.payment_amount,0.0)
from dwd_dim_base_province pro
left join tmp_login on pro.area_code=tmp_login.area_code
left join tmp_op on pro.id=tmp_op.province_id;

```

## 数仓搭建-DWT层

DWT层一般是在DWS层进行进一步的聚合，例如7天、15天、1个月、迄今为止这种层面的聚合数据。在加载DWT层数据时，既要充分利用DWS层的轻度聚合数据，又要充分利用已有的DWT数据。

### 设备主题宽表

设备主题宽表统计的截止昨天，所有设备的首次活跃时间、末次活跃时间、当天活跃时间、当天活跃次数、累计活跃次数。

```sql
drop table if exists dwt_uv_topic;
create external table dwt_uv_topic
(
    `mid_id` string comment '设备id',
    `brand` string comment '手机品牌',
    `model` string comment '手机型号',
    `login_date_first` string  comment '首次活跃时间',
    `login_date_last` string  comment '末次活跃时间',
    `login_day_count` bigint comment '当日活跃次数',
    `login_count` bigint comment '累积活跃天数'
) COMMENT '设备主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_uv_topic'
tblproperties ("parquet.compression"="lzo");

```

```sql
insert overwrite table dwt_uv_topic
select
    nvl(new.mid_id,old.mid_id),
    nvl(new.model,old.model),
    nvl(new.brand,old.brand),
    if(old.mid_id is null,'2020-06-14',old.login_date_first),
    if(new.mid_id is not null,'2020-06-14',old.login_date_last),
    if(new.mid_id is not null, new.login_count,0),
    nvl(old.login_count,0)+if(new.login_count>0,1,0)
from
(
    select
        *
    from dwt_uv_topic
)old
full outer join
(
    select
        *
    from dws_uv_detail_daycount
    where dt='2020-06-14'
)new
on old.mid_id=new.mid_id;


```

### 会员主题宽表

会员主题宽表统计的近30天/累计的登录数据、下单数据、支付数据。

```sql
create external table dwt_user_topic
(
    user_id string  comment '用户id',
    login_date_first string  comment '首次登录时间',
    login_date_last string  comment '末次登录时间',
    login_count bigint comment '累积登录天数',
    login_last_30d_count bigint comment '最近30日登录天数',
    order_date_first string  comment '首次下单时间',
    order_date_last string  comment '末次下单时间',
    order_count bigint comment '累积下单次数',
    order_amount decimal(16,2) comment '累积下单金额',
    order_last_30d_count bigint comment '最近30日下单次数',
    order_last_30d_amount bigint comment '最近30日下单金额',
    payment_date_first string  comment '首次支付时间',
    payment_date_last string  comment '末次支付时间',
    payment_count decimal(16,2) comment '累积支付次数',
    payment_amount decimal(16,2) comment '累积支付金额',
    payment_last_30d_count decimal(16,2) comment '最近30日支付次数',
    payment_last_30d_amount decimal(16,2) comment '最近30日支付金额'
)COMMENT '会员主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_user_topic/'
tblproperties ("parquet.compression"="lzo");

```

```sql
insert overwrite table dwt_user_topic
select
    nvl(new.user_id,old.user_id),
    if(old.login_date_first is null and new.login_count>0,'2020-06-14',old.login_date_first),
    if(new.login_count>0,'2020-06-14',old.login_date_last),
    nvl(old.login_count,0)+if(new.login_count>0,1,0),
    nvl(new.login_last_30d_count,0),
    if(old.order_date_first is null and new.order_count>0,'2020-06-14',old.order_date_first),
    if(new.order_count>0,'2020-06-14',old.order_date_last),
    nvl(old.order_count,0)+nvl(new.order_count,0),
    nvl(old.order_amount,0)+nvl(new.order_amount,0),
    nvl(new.order_last_30d_count,0),
    nvl(new.order_last_30d_amount,0),
    if(old.payment_date_first is null and new.payment_count>0,'2020-06-14',old.payment_date_first),
    if(new.payment_count>0,'2020-06-14',old.payment_date_last),
    nvl(old.payment_count,0)+nvl(new.payment_count,0),
    nvl(old.payment_amount,0)+nvl(new.payment_amount,0),
    nvl(new.payment_last_30d_count,0),
    nvl(new.payment_last_30d_amount,0)
from
dwt_user_topic old
full outer join
(
    select
        user_id,
        sum(if(dt='2020-06-14',login_count,0)) login_count,
        sum(if(dt='2020-06-14',order_count,0)) order_count,
        sum(if(dt='2020-06-14',order_amount,0)) order_amount,
        sum(if(dt='2020-06-14',payment_count,0)) payment_count,
        sum(if(dt='2020-06-14',payment_amount,0)) payment_amount,
        sum(if(login_count>0,1,0)) login_last_30d_count,
        sum(order_count) order_last_30d_count,
        sum(order_amount) order_last_30d_amount,
        sum(payment_count) payment_last_30d_count,
        sum(payment_amount) payment_last_30d_amount
    from dws_user_action_daycount
    where dt>=date_add( '2020-06-14',-30)
    group by user_id
)new
on old.user_id=new.user_id;

```

### 商品主题宽表

商品主题宽表统计的是商品近30天/累计被下单数据、被支付数据、被退款数据、被评价数据。

```sql
drop table if exists dwt_sku_topic;
create external table dwt_sku_topic
(
    sku_id string comment 'sku_id',
    spu_id string comment 'spu_id',
    order_last_30d_count bigint comment '最近30日被下单次数',
    order_last_30d_num bigint comment '最近30日被下单件数',
    order_last_30d_amount decimal(16,2)  comment '最近30日被下单金额',
    order_count bigint comment '累积被下单次数',
    order_num bigint comment '累积被下单件数',
    order_amount decimal(16,2) comment '累积被下单金额',
    payment_last_30d_count   bigint  comment '最近30日被支付次数',
    payment_last_30d_num bigint comment '最近30日被支付件数',
    payment_last_30d_amount  decimal(16,2) comment '最近30日被支付金额',
    payment_count   bigint  comment '累积被支付次数',
    payment_num bigint comment '累积被支付件数',
    payment_amount  decimal(16,2) comment '累积被支付金额',
    refund_last_30d_count bigint comment '最近三十日退款次数',
    refund_last_30d_num bigint comment '最近三十日退款件数',
    refund_last_30d_amount decimal(16,2) comment '最近三十日退款金额',
    refund_count bigint comment '累积退款次数',
    refund_num bigint comment '累积退款件数',
    refund_amount decimal(16,2) comment '累积退款金额',
    cart_last_30d_count bigint comment '最近30日被加入购物车次数',
    cart_count bigint comment '累积被加入购物车次数',
    favor_last_30d_count bigint comment '最近30日被收藏次数',
    favor_count bigint comment '累积被收藏次数',
    appraise_last_30d_good_count bigint comment '最近30日好评数',
    appraise_last_30d_mid_count bigint comment '最近30日中评数',
    appraise_last_30d_bad_count bigint comment '最近30日差评数',
    appraise_last_30d_default_count bigint comment '最近30日默认评价数',
    appraise_good_count bigint comment '累积好评数',
    appraise_mid_count bigint comment '累积中评数',
    appraise_bad_count bigint comment '累积差评数',
    appraise_default_count bigint comment '累积默认评价数'
 )COMMENT '商品主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_sku_topic/'
tblproperties ("parquet.compression"="lzo");

```

```sql
insert overwrite table dwt_sku_topic
select 
    nvl(new.sku_id,old.sku_id),
    sku_info.spu_id,
    nvl(new.order_count30,0),
    nvl(new.order_num30,0),
    nvl(new.order_amount30,0),
    nvl(old.order_count,0) + nvl(new.order_count,0),
    nvl(old.order_num,0) + nvl(new.order_num,0),
    nvl(old.order_amount,0) + nvl(new.order_amount,0),
    nvl(new.payment_count30,0),
    nvl(new.payment_num30,0),
    nvl(new.payment_amount30,0),
    nvl(old.payment_count,0) + nvl(new.payment_count,0),
    nvl(old.payment_num,0) + nvl(new.payment_count,0),
    nvl(old.payment_amount,0) + nvl(new.payment_count,0),
    nvl(new.refund_count30,0),
    nvl(new.refund_num30,0),
    nvl(new.refund_amount30,0),
    nvl(old.refund_count,0) + nvl(new.refund_count,0),
    nvl(old.refund_num,0) + nvl(new.refund_num,0),
    nvl(old.refund_amount,0) + nvl(new.refund_amount,0),
    nvl(new.cart_count30,0),
    nvl(old.cart_count,0) + nvl(new.cart_count,0),
    nvl(new.favor_count30,0),
    nvl(old.favor_count,0) + nvl(new.favor_count,0),
    nvl(new.appraise_good_count30,0),
    nvl(new.appraise_mid_count30,0),
    nvl(new.appraise_bad_count30,0),
    nvl(new.appraise_default_count30,0)  ,
    nvl(old.appraise_good_count,0) + nvl(new.appraise_good_count,0),
    nvl(old.appraise_mid_count,0) + nvl(new.appraise_mid_count,0),
    nvl(old.appraise_bad_count,0) + nvl(new.appraise_bad_count,0),
    nvl(old.appraise_default_count,0) + nvl(new.appraise_default_count,0) 
from 
dwt_sku_topic old
full outer join 
(
    select 
        sku_id,
        sum(if(dt='2020-06-14', order_count,0 )) order_count,
        sum(if(dt='2020-06-14',order_num ,0 ))  order_num, 
        sum(if(dt='2020-06-14',order_amount,0 )) order_amount ,
        sum(if(dt='2020-06-14',payment_count,0 )) payment_count,
        sum(if(dt='2020-06-14',payment_num,0 )) payment_num,
        sum(if(dt='2020-06-14',payment_amount,0 )) payment_amount,
        sum(if(dt='2020-06-14',refund_count,0 )) refund_count,
        sum(if(dt='2020-06-14',refund_num,0 )) refund_num,
        sum(if(dt='2020-06-14',refund_amount,0 )) refund_amount,  
        sum(if(dt='2020-06-14',cart_count,0 )) cart_count,
        sum(if(dt='2020-06-14',favor_count,0 )) favor_count,
        sum(if(dt='2020-06-14',appraise_good_count,0 )) appraise_good_count,  
        sum(if(dt='2020-06-14',appraise_mid_count,0 ) ) appraise_mid_count ,
        sum(if(dt='2020-06-14',appraise_bad_count,0 )) appraise_bad_count,  
        sum(if(dt='2020-06-14',appraise_default_count,0 )) appraise_default_count,
        sum(order_count) order_count30 ,
        sum(order_num) order_num30,
        sum(order_amount) order_amount30,
        sum(payment_count) payment_count30,
        sum(payment_num) payment_num30,
        sum(payment_amount) payment_amount30,
        sum(refund_count) refund_count30,
        sum(refund_num) refund_num30,
        sum(refund_amount) refund_amount30,
        sum(cart_count) cart_count30,
        sum(favor_count) favor_count30,
        sum(appraise_good_count) appraise_good_count30,
        sum(appraise_mid_count) appraise_mid_count30,
        sum(appraise_bad_count) appraise_bad_count30,
        sum(appraise_default_count) appraise_default_count30 
    from dws_sku_action_daycount
    where dt >= date_add ('2020-06-14', -30)
    group by sku_id    
)new 
on new.sku_id = old.sku_id
left join 
(select * from dwd_dim_sku_info where dt='2020-06-14') sku_info
on nvl(new.sku_id,old.sku_id)= sku_info.id;

```

### 活动主题宽表

活动主题宽表统计的是当天/累计的下单数据、支付数据。

```sql
drop table if exists dwt_activity_topic;
create external table dwt_activity_topic(
    `id` string COMMENT '编号',
    `activity_name` string  COMMENT '活动名称',
    `activity_type` string  COMMENT '活动类型',
    `start_time` string  COMMENT '开始时间',
    `end_time` string  COMMENT '结束时间',
    `create_time` string  COMMENT '创建时间',
    `display_day_count` bigint COMMENT '当日曝光次数',
    `order_day_count` bigint COMMENT '当日下单次数',
    `order_day_amount` decimal(20,2) COMMENT '当日下单金额',
    `payment_day_count` bigint COMMENT '当日支付次数',
    `payment_day_amount` decimal(20,2) COMMENT '当日支付金额',
    `display_count` bigint COMMENT '累积曝光次数',
    `order_count` bigint COMMENT '累积下单次数',
    `order_amount` decimal(20,2) COMMENT '累积下单金额',
    `payment_count` bigint COMMENT '累积支付次数',
    `payment_amount` decimal(20,2) COMMENT '累积支付金额'
) COMMENT '活动主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_activity_topic/'
tblproperties ("parquet.compression"="lzo");

```

```sql
insert overwrite table dwt_activity_topic
select
    nvl(new.id,old.id),
    nvl(new.activity_name,old.activity_name),
    nvl(new.activity_type,old.activity_type),
    nvl(new.start_time,old.start_time),
    nvl(new.end_time,old.end_time),
    nvl(new.create_time,old.create_time),
    nvl(new.display_count,0),
    nvl(new.order_count,0),
    nvl(new.order_amount,0.0),
    nvl(new.payment_count,0),
    nvl(new.payment_amount,0.0),
    nvl(new.display_count,0)+nvl(old.display_count,0),
    nvl(new.order_count,0)+nvl(old.order_count,0),
    nvl(new.order_amount,0.0)+nvl(old.order_amount,0.0),
    nvl(new.payment_count,0)+nvl(old.payment_count,0),
    nvl(new.payment_amount,0.0)+nvl(old.payment_amount,0.0)
from
(
    select
        *
    from dwt_activity_topic
)old
full outer join
(
    select
        *
    from dws_activity_info_daycount
    where dt='2020-06-14'
)new
on old.id=new.id;

```

### 地区主题宽表

地区主题宽表统计的是当天、近30天的设备活跃、下单、支付数据。

```sql
drop table if exists dwt_area_topic;
create external table dwt_area_topic(
    `id` bigint COMMENT '编号',
    `province_name` string COMMENT '省份名称',
    `area_code` string COMMENT '地区编码',
    `iso_code` string COMMENT 'iso编码',
    `region_id` string COMMENT '地区ID',
    `region_name` string COMMENT '地区名称',
    `login_day_count` string COMMENT '当天活跃设备数',
    `login_last_30d_count` string COMMENT '最近30天活跃设备数',
    `order_day_count` bigint COMMENT '当天下单次数',
    `order_day_amount` decimal(16,2) COMMENT '当天下单金额',
    `order_last_30d_count` bigint COMMENT '最近30天下单次数',
    `order_last_30d_amount` decimal(16,2) COMMENT '最近30天下单金额',
    `payment_day_count` bigint COMMENT '当天支付次数',
    `payment_day_amount` decimal(16,2) COMMENT '当天支付金额',
    `payment_last_30d_count` bigint COMMENT '最近30天支付次数',
    `payment_last_30d_amount` decimal(16,2) COMMENT '最近30天支付金额'
) COMMENT '地区主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_area_topic/'
tblproperties ("parquet.compression"="lzo");

```

```sql
insert overwrite table dwt_area_topic
select
    nvl(old.id,new.id),
    nvl(old.province_name,new.province_name),
    nvl(old.area_code,new.area_code),
    nvl(old.iso_code,new.iso_code),
    nvl(old.region_id,new.region_id),
    nvl(old.region_name,new.region_name),
    nvl(new.login_day_count,0),
    nvl(new.login_last_30d_count,0),
    nvl(new.order_day_count,0),
    nvl(new.order_day_amount,0.0),
    nvl(new.order_last_30d_count,0),
    nvl(new.order_last_30d_amount,0.0),
    nvl(new.payment_day_count,0),
    nvl(new.payment_day_amount,0.0),
    nvl(new.payment_last_30d_count,0),
    nvl(new.payment_last_30d_amount,0.0)
from 
(
    select
        *
    from dwt_area_topic
)old
full outer join
(
    select
        id,
        province_name,
        area_code,
        iso_code,
        region_id,
        region_name,
        sum(if(dt='2020-06-14',login_count,0)) login_day_count,
        sum(if(dt='2020-06-14',order_count,0)) order_day_count,
        sum(if(dt='2020-06-14',order_amount,0.0)) order_day_amount,
        sum(if(dt='2020-06-14',payment_count,0)) payment_day_count,
        sum(if(dt='2020-06-14',payment_amount,0.0)) payment_day_amount,
        sum(login_count) login_last_30d_count,
        sum(order_count) order_last_30d_count,
        sum(order_amount) order_last_30d_amount,
        sum(payment_count) payment_last_30d_count,
        sum(payment_amount) payment_last_30d_amount
    from dws_area_stats_daycount
    where dt>=date_add('2020-06-14',-30)
    group by id,province_name,area_code,iso_code,region_id,region_name
)new
on old.id=new.id;

```

## 数仓搭建-ADS层

#### 建表说明

ADS层不涉及建模，建表根据具体需求而定。

#### 设备主题

##### 活跃设备数（日、周、月）

需求定义：

日活：当日活跃的**设备数**

周活：当周活跃的**设备数**

月活：当月活跃的**设备数**

1）建表语句

```sql
drop table if exists ads_uv_count;

create external table ads_uv_count(

  `dt` string COMMENT '统计日期',

  `day_count` bigint COMMENT '当日用户数量',

  `wk_count` bigint COMMENT '当周用户数量',

  `mn_count` bigint COMMENT '当月用户数量',

  `is_weekend` string COMMENT 'Y,N是否是周末,用于得到本周最终结果',

  `is_monthend` string COMMENT 'Y,N是否是月末,用于得到本月最终结果' 

) COMMENT '活跃设备数'

row format delimited fields terminated by '\t'

location '/warehouse/gmall/ads/ads_uv_count/';

```



2）导入数据

```sql
insert into table ads_uv_count 

select 

  '2020-06-14' dt,

  daycount.ct,

  wkcount.ct,

  mncount.ct,

  if(date_add(next_day('2020-06-14','MO'),-1)='2020-06-14','Y','N') ,

  if(last_day('2020-06-14')='2020-06-14','Y','N') 

from (

  select 

   '2020-06-14' dt,

   count(*) ct

  from dwt_uv_topic

  where login_date_last='2020-06-14' 

)daycount join( 

  select 

    '2020-06-14' dt,

    count (*) ct

  from dwt_uv_topic

  where login_date_last>=date_add(next_day('2020-06-14','MO'),-7) 

  and login_date_last<= date_add(next_day('2020-06-14','MO'),-1) 

) wkcount 
	on daycount.dt=wkcount.dt

join ( 

  select 

   '2020-06-14' dt,

    count (*) ct

  from dwt_uv_topic

  where date_format(login_date_last,'yyyy-MM')=date_format('2020-06-14','yyyy-MM') 

)mncount on daycount.dt=mncount.dt;

```

##### 每日新增设备

1）建表语句

```sql
drop table if exists ads_new_mid_count;

create external table ads_new_mid_count

(

  `create_date`   string comment '创建时间' ,

  `new_mid_count`  BIGINT comment '新增设备数量' 

) COMMENT '每日新增设备数量'

row format delimited fields terminated by '\t'

location '/warehouse/gmall/ads/ads_new_mid_count/';

```

2）导入数据

```sql
insert into table ads_new_mid_count 

select

  '2020-06-14',

  count(*)

from dwt_uv_topic

where login_date_first='2020-06-14';

```

##### 留存率       

1） 建表语句

计算1天/2天/3天前注册到今天的留存率

```sql
drop table if exists ads_user_retention_day_rate;

create external table ads_user_retention_day_rate 

(

   `stat_date`     string comment '统计日期',

   `create_date`    string comment '设备新增日期',

   `retention_day`   int comment '截止当前日期留存天数',

   `retention_count`  bigint comment '留存数量',

   `new_mid_count`   bigint comment '设备新增数量',

   `retention_ratio`  decimal(16,2) comment '留存率'

) COMMENT '留存率'

row format delimited fields terminated by '\t'

location '/warehouse/gmall/ads/ads_user_retention_day_rate/';

```

2）导入数据

```sql
insert into table ads_user_retention_day_rate

select

  '2020-06-15',

  date_add('2020-06-15',-1),

  1,--留存天数

  sum(if(login_date_first=date_add('2020-06-15',-1) and login_date_last='2020-06-15',1,0)),

  sum(if(login_date_first=date_add('2020-06-15',-1),1,0)),

  sum(if(login_date_first=date_add('2020-06-15',-1) and login_date_last='2020-06-15',1,0))/sum(if(login_date_first=date_add('2020-06-15',-1),1,0))*100

from dwt_uv_topic

 

union all

 

select

  '2020-06-15',

  date_add('2020-06-15',-2),

  2,

  sum(if(login_date_first=date_add('2020-06-15',-2) and login_date_last='2020-06-15',1,0)),

  sum(if(login_date_first=date_add('2020-06-15',-2),1,0)),

  sum(if(login_date_first=date_add('2020-06-15',-2) and login_date_last='2020-06-15',1,0))/sum(if(login_date_first=date_add('2020-06-15',-2),1,0))*100

from dwt_uv_topic

 

union all

 

select

  '2020-06-15',

  date_add('2020-06-15',-3),

  3,

  sum(if(login_date_first=date_add('2020-06-15',-3) and login_date_last='2020-06-15',1,0)),

  sum(if(login_date_first=date_add('2020-06-15',-3),1,0)),

  sum(if(login_date_first=date_add('2020-06-15',-3) and login_date_last='2020-06-15',1,0))/sum(if(login_date_first=date_add('2020-06-15',-3),1,0))*100

from dwt_uv_topic;

```

##### 沉默用户数

需求定义：

沉默用户：只在安装当天启动过，且启动时间是在7天前

1）建表语句

```sql
drop table if exists ads_silent_count;

create external table ads_silent_count( 

  `dt` string COMMENT '统计日期',

  `silent_count` bigint COMMENT '沉默设备数'

) COMMENT '沉默用户数'

row format delimited fields terminated by '\t'

location '/warehouse/gmall/ads/ads_silent_count';

```

2）导入2020-06-25数据

```sql
insert into table ads_silent_count

select

  '2020-06-25',

  count(*) 

from dwt_uv_topic

where login_date_first=login_date_last

and login_date_last<=date_add('2020-06-25',-7);

```

##### 本周回流用户数

需求定义：

本周回流用户：上周未活跃，本周活跃的设备，且不是本周新增设备

1）建表语句

```sql
drop table if exists ads_back_count;

create external table ads_back_count( 

  `dt` string COMMENT '统计日期',

  `wk_dt` string COMMENT '统计日期所在周',

  `wastage_count` bigint COMMENT '回流设备数'

) COMMENT '本周回流用户数'

row format delimited fields terminated by '\t'

location '/warehouse/gmall/ads/ads_back_count';

```

2）导入数据：

```sql
insert into table ads_back_count

select

  '2020-06-25',

  concat(date_add(next_day('2020-06-25','MO'),-7),'_', date_add(next_day('2020-06-25','MO'),-1)),

  count(*)

from

(

  select

    mid_id

  from dwt_uv_topic

  where login_date_last>=date_add(next_day('2020-06-25','MO'),-7) 

  and login_date_last<= date_add(next_day('2020-06-25','MO'),-1)

  and login_date_first<date_add(next_day('2020-06-25','MO'),-7)

)current_wk

left join

(

  select

    mid_id

  from dws_uv_detail_daycount

  where dt>=date_add(next_day('2020-06-25','MO'),-7*2) 

  and dt<= date_add(next_day('2020-06-25','MO'),-7-1) 

  group by mid_id

)last_wk

on current_wk.mid_id=last_wk.mid_id

where last_wk.mid_id is null;
```

##### 流失用户数

需求定义：

流失用户：最近7天未活跃的设备

1）建表语句

```sql
drop table if exists ads_wastage_count;

create external table ads_wastage_count( 

  `dt` string COMMENT '统计日期',

  `wastage_count` bigint COMMENT '流失设备数'

) COMMENT '流失用户数'

row format delimited fields terminated by '\t'

location '/warehouse/gmall/ads/ads_wastage_count';
```

2）导入2020-06-25数据

```sql
insert into table ads_wastage_count

select

   '2020-06-25',

   count(*)

from 

(

  select 

    mid_id

  from dwt_uv_topic

  where login_date_last<=date_add('2020-06-25',-7)

  group by mid_id

)t1;

```

##### 最近连续三周活跃用户数

1）建表语句

```sql
drop table if exists ads_continuity_wk_count;

create external table ads_continuity_wk_count( 

  `dt` string COMMENT '统计日期,一般用结束周周日日期,如果每天计算一次,可用当天日期',

  `wk_dt` string COMMENT '持续时间',

  `continuity_count` bigint COMMENT '活跃用户数'

) COMMENT '最近连续三周活跃用户数'

row format delimited fields terminated by '\t'

location '/warehouse/gmall/ads/ads_continuity_wk_count';

```

2）导入2020-06-25所在周的数据

```sql
insert into table ads_continuity_wk_count

select

  '2020-06-25',

  concat(date_add(next_day('2020-06-25','MO'),-7*3),'_',date_add(next_day('2020-06-25','MO'),-1)),

  count(*)

from

(

  select

​    mid_id

  from

  (

​    select

​      mid_id

​    from dws_uv_detail_daycount

​    where dt>=date_add(next_day('2020-06-25','monday'),-7)

​    and dt<=date_add(next_day('2020-06-25','monday'),-1)

​    group by mid_id

 

​    union all

 

​    select

​      mid_id

​    from dws_uv_detail_daycount

​    where dt>=date_add(next_day('2020-06-25','monday'),-7*2)

​    and dt<=date_add(next_day('2020-06-25','monday'),-7-1)

​    group by mid_id

 

​    union all

 

​    select

​      mid_id

​    from dws_uv_detail_daycount

​    where dt>=date_add(next_day('2020-06-25','monday'),-7*3)

​    and dt<=date_add(next_day('2020-06-25','monday'),-7*2-1)

​    group by mid_id

  )t1

  group by mid_id

  having count(*)=3

)t2;

```

##### 最近七天内连续三天活跃用户数

1）建表语句

```sql
drop table if exists ads_continuity_uv_count;

create external table ads_continuity_uv_count( 

  `dt` string COMMENT '统计日期',

  `wk_dt` string COMMENT '最近7天日期',

  `continuity_count` bigint

) COMMENT '最近七天内连续三天活跃用户数'

row format delimited fields terminated by '\t'

location '/warehouse/gmall/ads/ads_continuity_uv_count';

```

2） 导入数据

```sql
insert into table ads_continuity_uv_count

select

  '2020-06-16',

  concat(date_add('2020-06-16',-6),'_','2020-06-16'),

  count(*)

from

(

  select mid_id

  from

  (

​    select mid_id

​    from

​    (

​      select 

​        mid_id,

​        date_sub(dt,rank) date_dif

​      from

​      (

​        select

​          mid_id,

​          dt,

​          rank() over(partition by mid_id order by dt) rank

​        from dws_uv_detail_daycount

​        where dt>=date_add('2020-06-16',-6) and dt<='2020-06-16'

​      )t1

​    )t2 

​    group by mid_id,date_dif

​     having count(*)>=3

  )t3 

  group by mid_id

)t4;

```

#### 会员主题

#####  会员信息

1）建表

```sql
drop table if exists ads_user_topic;

create external table ads_user_topic(

  `dt` string COMMENT '统计日期',

  `day_users` string COMMENT '活跃会员数',

  `day_new_users` string COMMENT '新增会员数',

  `day_new_payment_users` string COMMENT '新增消费会员数',

  `payment_users` string COMMENT '总付费会员数',

  `users` string COMMENT '总会员数',

  `day_users2users` decimal(16,2) COMMENT '会员活跃率',

  `payment_users2users` decimal(16,2) COMMENT '会员付费率',

  `day_new_users2users` decimal(16,2) COMMENT '会员新鲜度'

) COMMENT '会员信息表'

row format delimited fields terminated by '\t'

location '/warehouse/gmall/ads/ads_user_topic';

```

2）导入数据

```sql
insert into table ads_user_topic

select

  '2020-06-14',

  sum(if(login_date_last='2020-06-14',1,0)),

  sum(if(login_date_first='2020-06-14',1,0)),

  sum(if(payment_date_first='2020-06-14',1,0)),

  sum(if(payment_count>0,1,0)),

  count(*),

  sum(if(login_date_last='2020-06-14',1,0))/count(*),

  sum(if(payment_count>0,1,0))/count(*),

  sum(if(login_date_first='2020-06-14',1,0))/sum(if(login_date_last='2020-06-14',1,0))

from dwt_user_topic;

```

##### 漏斗分析

统计“浏览首页->浏览商品详情页->加入购物车->下单->支付”的转化率

思路：统计各个行为的人数，然后计算比值。

1）建表语句

```sql
drop table if exists ads_user_action_convert_day;

create external table ads_user_action_convert_day(

  `dt` string COMMENT '统计日期',

  `home_count` bigint COMMENT '浏览首页人数',

  `good_detail_count` bigint COMMENT '浏览商品详情页人数',

  `home2good_detail_convert_ratio` decimal(16,2) COMMENT '首页到商品详情转化率',

  `cart_count` bigint COMMENT '加入购物车的人数',

  `good_detail2cart_convert_ratio` decimal(16,2) COMMENT '商品详情页到加入购物车转化率',

  `order_count` bigint   COMMENT '下单人数',

  `cart2order_convert_ratio` decimal(16,2) COMMENT '加入购物车到下单转化率',

  `payment_amount` bigint   COMMENT '支付人数',

  `order2payment_convert_ratio` decimal(16,2) COMMENT '下单到支付的转化率'

) COMMENT '漏斗分析'

row format delimited fields terminated by '\t'

location '/warehouse/gmall/ads/ads_user_action_convert_day/';

```

2）数据装载

```sql
with

tmp_uv as

(

  select

​    '2020-06-14' dt,

​    sum(if(array_contains(pages,'home'),1,0)) home_count,

​    sum(if(array_contains(pages,'good_detail'),1,0)) good_detail_count

  from

  (

​    select

​      mid_id,

​      collect_set(page_id) pages

​    from dwd_page_log

​    where dt='2020-06-14'

​    and page_id in ('home','good_detail')

​    group by mid_id

  )tmp

),

tmp_cop as

(

  select 

​    '2020-06-14' dt,

​    sum(if(cart_count>0,1,0)) cart_count,

​    sum(if(order_count>0,1,0)) order_count,

​    sum(if(payment_count>0,1,0)) payment_count

  from dws_user_action_daycount

  where dt='2020-06-14'

)

insert into table ads_user_action_convert_day

select

  tmp_uv.dt,

  tmp_uv.home_count,

  tmp_uv.good_detail_count,

  tmp_uv.good_detail_count/tmp_uv.home_count*100,

  tmp_cop.cart_count,

  tmp_cop.cart_count/tmp_uv.good_detail_count*100,

  tmp_cop.order_count,

  tmp_cop.order_count/tmp_cop.cart_count*100,

  tmp_cop.payment_count,

  tmp_cop.payment_count/tmp_cop.order_count*100

from tmp_uv

join tmp_cop

on tmp_uv.dt=tmp_cop.dt;
```

#### 商品主题

#####  商品个数信息

1）建表语句

```sql
drop table if exists ads_product_info;

create external table ads_product_info(

  `dt` string COMMENT '统计日期',

  `sku_num` string COMMENT 'sku个数',

  `spu_num` string COMMENT 'spu个数'

) COMMENT '商品个数信息'

row format delimited fields terminated by '\t'

location '/warehouse/gmall/ads/ads_product_info';
```

2）导入数据

```sql
insert into table ads_product_info

select

  '2020-06-14' dt,

  sku_num,

  spu_num

from

(

  select

​    '2020-06-14' dt,

​    count(*) sku_num

  from

​    dwt_sku_topic

) tmp_sku_num

join

(

  select

​    '2020-06-14' dt,

​    count(*) spu_num

  from

  (

​    select

​      spu_id

​    from

​      dwt_sku_topic

​    group by

​      spu_id

  ) tmp_spu_id

) tmp_spu_num

on tmp_sku_num.dt=tmp_spu_num.dt;

```

##### 商品销量排名

1）建表语句

```sql
drop table if exists ads_product_sale_topN;

create external table ads_product_sale_topN(

  `dt` string COMMENT '统计日期',

  `sku_id` string COMMENT '商品ID',

  `payment_amount` bigint COMMENT '销量'

) COMMENT '商品销量排名'

row format delimited fields terminated by '\t'

location '/warehouse/gmall/ads/ads_product_sale_topN';

```

2）导入数据

```sql
insert into table ads_product_sale_topN

select

  '2020-06-14' dt,

  sku_id,

  payment_amount

from

  dws_sku_action_daycount

where

  dt='2020-06-14'

order by payment_amount desc

limit 10;

```

#####  商品收藏排名

1）建表语句

```sql
drop table if exists ads_product_favor_topN;

create external table ads_product_favor_topN(

  `dt` string COMMENT '统计日期',

  `sku_id` string COMMENT '商品ID',

  `favor_count` bigint COMMENT '收藏量'

) COMMENT '商品收藏排名'

row format delimited fields terminated by '\t'

location '/warehouse/gmall/ads/ads_product_favor_topN';

```

2）导入数据

```sql
insert into table ads_product_favor_topN

select

  '2020-06-14' dt,

  sku_id,

  favor_count

from

  dws_sku_action_daycount

where

  dt='2020-06-14'

order by favor_count desc

limit 10;

```

##### 商品加入购物车排名

1）建表语句

```sql
drop table if exists ads_product_cart_topN;

create external table ads_product_cart_topN(

  `dt` string COMMENT '统计日期',

  `sku_id` string COMMENT '商品ID',

  `cart_count` bigint COMMENT '加入购物车次数'

) COMMENT '商品加入购物车排名'

row format delimited fields terminated by '\t'

location '/warehouse/gmall/ads/ads_product_cart_topN';

```

2）导入数据

```sql
insert into table ads_product_cart_topN

select

  '2020-06-14' dt,

  sku_id,

  cart_count

from

  dws_sku_action_daycount

where

  dt='2020-06-14'

order by cart_count desc

limit 10;

```

##### 商品退款率排名（最近30天）

1）建表语句

```sql
drop table if exists ads_product_refund_topN;

create external table ads_product_refund_topN(

  `dt` string COMMENT '统计日期',

  `sku_id` string COMMENT '商品ID',

  `refund_ratio` decimal(16,2) COMMENT '退款率'

) COMMENT '商品退款率排名'

row format delimited fields terminated by '\t'

location '/warehouse/gmall/ads/ads_product_refund_topN';

```

2）导入数据

```sql
insert into table ads_product_refund_topN

select

  '2020-06-14',

  sku_id,

  refund_last_30d_count/payment_last_30d_count*100 refund_ratio

from dwt_sku_topic

order by refund_ratio desc

limit 10;

```

#####  商品差评率

1）建表语句

```sql
drop table if exists ads_appraise_bad_topN;

create external table ads_appraise_bad_topN(

  `dt` string COMMENT '统计日期',

  `sku_id` string COMMENT '商品ID',

  `appraise_bad_ratio` decimal(16,2) COMMENT '差评率'

) COMMENT '商品差评率'

row format delimited fields terminated by '\t'

location '/warehouse/gmall/ads/ads_appraise_bad_topN';

```

2）导入数据

```sql
insert into table ads_appraise_bad_topN

select

  '2020-06-14' dt,

  sku_id,

appraise_bad_count/(appraise_good_count+appraise_mid_count+appraise_bad_count+appraise_default_count) appraise_bad_ratio

from

  dws_sku_action_daycount

where

  dt='2020-06-14'

order by appraise_bad_ratio desc

limit 10;

```

#### 营销主题（用户+商品+购买行为）

##### 下单数目统计

需求分析：统计每日下单数，下单金额及下单用户数。

```sql
drop table if exists ads_order_daycount;

create external table ads_order_daycount(

  dt string comment '统计日期',

  order_count bigint comment '单日下单笔数',

  order_amount bigint comment '单日下单金额',

  order_users bigint comment '单日下单用户数'

) comment '下单数目统计'

row format delimited fields terminated by '\t'

location '/warehouse/gmall/ads/ads_order_daycount';

```

2）导入数据

```sql
insert into table ads_order_daycount

select

  '2020-06-14',

  sum(order_count),

  sum(order_amount),

  sum(if(order_count>0,1,0))

from dws_user_action_daycount

where dt='2020-06-14';

```

#####  支付信息统计

每日支付金额、支付人数、支付商品数、支付笔数以及下单到支付的平均时长（取自DWD）

1）建表

```sql
drop table if exists ads_payment_daycount;

create external table ads_payment_daycount(

  dt string comment '统计日期',

  order_count bigint comment '单日支付笔数',

  order_amount bigint comment '单日支付金额',

  payment_user_count bigint comment '单日支付人数',

  payment_sku_count bigint comment '单日支付商品数',

  payment_avg_time decimal(16,2) comment '下单到支付的平均时长，取分钟数'

) comment '支付信息统计'

row format delimited fields terminated by '\t'

location '/warehouse/gmall/ads/ads_payment_daycount';

```

2）导入数据

```sql
insert into table ads_payment_daycount

select

  tmp_payment.dt,

  tmp_payment.payment_count,

  tmp_payment.payment_amount,

  tmp_payment.payment_user_count,

  tmp_skucount.payment_sku_count,

  tmp_time.payment_avg_time

from

(

  select

​    '2020-06-14' dt,

​    sum(payment_count) payment_count,

​    sum(payment_amount) payment_amount,

​    sum(if(payment_count>0,1,0)) payment_user_count

  from dws_user_action_daycount

  where dt='2020-06-14'

)tmp_payment

join

(

  select

​    '2020-06-14' dt,

​    sum(if(payment_count>0,1,0)) payment_sku_count 

  from dws_sku_action_daycount

  where dt='2020-06-14'

)tmp_skucount on tmp_payment.dt=tmp_skucount.dt

join

(

  select

​    '2020-06-14' dt,

​    sum(unix_timestamp(payment_time)-unix_timestamp(create_time))/count(*)/60 payment_avg_time

  from dwd_fact_order_info

  where dt='2020-06-14'

  and payment_time is not null

)tmp_time on tmp_payment.dt=tmp_time.dt;

```

##### 品牌复购率

1）建表语句

```sql
drop table ads_sale_tm_category1_stat_mn;

create external table ads_sale_tm_category1_stat_mn

( 

  tm_id string comment '品牌id',

  category1_id string comment '1级品类id ',

  category1_name string comment '1级品类名称 ',

  buycount  bigint comment '购买人数',

  buy_twice_last bigint comment '两次以上购买人数',

  buy_twice_last_ratio decimal(16,2) comment '单次复购率',

  buy_3times_last  bigint comment  '三次以上购买人数',

  buy_3times_last_ratio decimal(16,2) comment '多次复购率',

  stat_mn string comment '统计月份',

  stat_date string comment '统计日期' 

) COMMENT '品牌复购率统计'

row format delimited fields terminated by '\t'

location '/warehouse/gmall/ads/ads_sale_tm_category1_stat_mn/';

```

2）数据导入

```sql
with 

tmp_order as

(

  select

​    user_id,

​    order_stats_struct.sku_id sku_id,

​    order_stats_struct.order_count order_count

  from dws_user_action_daycount lateral view explode(order_detail_stats) tmp as order_stats_struct

  where date_format(dt,'yyyy-MM')=date_format('2020-06-14','yyyy-MM')

),

tmp_sku as

(

  select

​    id,

​    tm_id,

​    category1_id,

​    category1_name

  from dwd_dim_sku_info

  where dt='2020-06-14'

)

insert into table ads_sale_tm_category1_stat_mn

select

  tm_id,

  category1_id,

  category1_name,

  sum(if(order_count>=1,1,0)) buycount,

  sum(if(order_count>=2,1,0)) buyTwiceLast,

  sum(if(order_count>=2,1,0))/sum( if(order_count>=1,1,0)) buyTwiceLastRatio,

  sum(if(order_count>=3,1,0)) buy3timeLast ,

  sum(if(order_count>=3,1,0))/sum( if(order_count>=1,1,0)) buy3timeLastRatio ,

  date_format('2020-06-14' ,'yyyy-MM') stat_mn,

  '2020-06-14' stat_date

from

(

  select 

​    tmp_order.user_id,

​    tmp_sku.category1_id,

​    tmp_sku.category1_name,

​    tmp_sku.tm_id,

​    sum(order_count) order_count

  from tmp_order

  join tmp_sku

  on tmp_order.sku_id=tmp_sku.id

  group by tmp_order.user_id,tmp_sku.category1_id,tmp_sku.category1_name,tmp_sku.tm_id

)tmp

group by tm_id, category1_id, category1_name;
```

#### 地区主题

##### 地区主题信息

1）建表语句

```sql
drop table if exists ads_area_topic;

create external table ads_area_topic(

  `dt` string COMMENT '统计日期',

  `id` bigint COMMENT '编号',

  `province_name` string COMMENT '省份名称',

  `area_code` string COMMENT '地区编码',

  `iso_code` string COMMENT 'iso编码',

  `region_id` string COMMENT '地区ID',

  `region_name` string COMMENT '地区名称',

  `login_day_count` bigint COMMENT '当天活跃设备数',

  `order_day_count` bigint COMMENT '当天下单次数',

  `order_day_amount` decimal(16,2) COMMENT '当天下单金额',

  `payment_day_count` bigint COMMENT '当天支付次数',

  `payment_day_amount` decimal(16,2) COMMENT '当天支付金额'

) COMMENT '地区主题信息'

row format delimited fields terminated by '\t'

location '/warehouse/gmall/ads/ads_area_topic/';

```

2）数据装载

```sql
insert into table ads_area_topic

select

  '2020-06-14',

  id,

  province_name,

  area_code,

  iso_code,

  region_id,

  region_name,

  login_day_count,

  order_day_count,

  order_day_amount,

  payment_day_count,

  payment_day_amount

from dwt_area_topic;

```

# Azkaban调度

## 说明

Azkaban是通过命令的返回值是否为0来判断该命令是否执行成功(0代表成功，其他代表失败)。但是，脚本中可能存在者不互相依赖的命令，例如多个建表命令在同一个脚本中并不互相依赖。这种情况下如果中间命令发生了错误，该脚本还是返回0，导致azkaban错以为该脚本执行成功。这导致寻找错误原因变得更加困难，也让失败自动尝试的机制失效。  解决方法有：

（1）每个脚本中只包含相互依赖的脚本。例如建表操作，每个建表操作作为一个脚本存在。

（2） 为脚本中不相关的命令添加依赖。例如建表操作前判断上一个命令是否返回0，如果不是，则跟者返回零，不执行建表操作。这样，只要有一个命令发生错误，那么该脚本就会返回非0值。

（3）在脚本的最后做一次检查，例如建表操作的脚本，在脚本最后查询下每张表是否存在，如果都存在，那么就返回01.

## .project

```sql
azkaban-flow-version: 2.0

```

## .flow

1. 所有标点符号的前面或者后面如果有内容，则中间一定有空格。  
2. 缩进代表层级，不允许用tab来代替空格进行缩进。

```
nodes:
  - name: mysql_to_hdfs
    type: command
    config:
     command: mysql_to_hdfs.sh all ${dt}
    
  - name: hdfs_to_ods_log
    type: command
    config:
     command: hdfs_to_ods_log.sh ${dt}
     
  - name: hdfs_to_ods_db
    type: command
    dependsOn: 
     - mysql_to_hdfs
    config: 
     command: hdfs_to_ods_db.sh all ${dt}
     
  - name: ods_to_dwd_log
    type: command
    dependsOn: 
     - hdfs_to_ods_log
    config: 
     command: ods_to_dwd_log.sh ${dt}
    
  - name: ods_to_dwd_db
    type: command
    dependsOn: 
     - hdfs_to_ods_db
    config: 
     command: ods_to_dwd_db.sh all ${dt}
    
  - name: dwd_to_dws
    type: command
    dependsOn:
     - ods_to_dwd_log
     - ods_to_dwd_db
    config:
     command: dwd_to_dws.sh ${dt}
    
  - name: dws_to_dwt
    type: command
    dependsOn:
     - dwd_to_dws
    config:
     command: dws_to_dwt.sh ${dt}
    
  - name: dwt_to_ads
    type: command
    dependsOn: 
     - dws_to_dwt
    config:
     command: dwt_to_ads.sh ${dt}
     
  - name: hdfs_to_mysql
    type: command
    dependsOn:
     - dwt_to_ads
    config:
      command: hdfs_to_mysql.sh all

```

# kylin:即时查询

## 原理

### 维度和度量

维度：即观察数据的角度。比如员工数据，可以从性别角度来分析，也可以更加细化，从入职时间或者地区的维度来观察。维度是一组离散的值，比如说性别中的男和女，或者时间维度上的每一个独立的日期。因此在统计时可以将维度值相同的记录聚合在一起，然后应用聚合函数做累加、平均、最大和最小值等聚合计算。

度量：即被聚合（观察）的统计值，也就是聚合运算的结果。比如说员工数据中不同性别员工的人数，又或者说在同一年入职的员工有多少。

### cube和cuboid

有了维度跟度量，一个数据表或者数据模型上的所有字段就可以分类了，它们要么是维度，要么是度量（可以被聚合）。于是就有了根据维度和度量做预计算的Cube理论。

给定一个数据模型，我们可以对其上的所有维度进行聚合，对于N个维度来说，组合`的所有可能性共有2n种。对于每一种维度的组合，将度量值做聚合计算，然后将结果保存为一个物化视图，称为Cuboid。所有维度组合的Cuboid作为一个整体，称为Cube。

下面举一个简单的例子说明，假设有一个电商的销售数据集，其中维度包括时间[time]、商品[item]、地区[location]和供应商[supplier]，度量为销售额。那么所有维度的组合就有24 = 16种，如下图所示：  

![1656053583957](C:\Users\neu_hgx\AppData\Roaming\Typora\typora-user-images\1656053583957.png)

一维度（1D）的组合有：[time]、[item]、[location]和[supplier]4种；

二维度（2D）的组合有：[time, item]、[time, location]、[time, supplier]、[item, location]、[item, supplier]、[location, supplier]3种；

三维度（3D）的组合也有4种；

最后还有零维度（0D）和四维度（4D）各有一种，总共16种。

注意：每一种维度组合就是一个Cuboid，16个Cuboid整体就是一个Cube。  

## 操作流程

1.选择数据源，指明要用到哪些表。

2.构建modele，指明其中哪些是事实表，哪些是维度表，哪些是度量值，哪些是维度值，哪些是分区字段，哪些是关联字段。

3.构建cube，指明预计算的逻辑，包括聚合方式（sum，avg等），指明分组（维度字段的组合）。

## 错误&解决

1.表中存在复杂数据类型导致报错。kylin不支持hive中的struct、array、map，因此当模型中包含复杂数据类型时会报错。

解决方法：创建view，剔除复杂数据类型。

2.重复key导致错误。每日全量维度表和拉链表中包含了多个版本信息，同一个设备id可能会有很多版本。这在星型模型中是不被允许的。解决方法：每日全量维度表建立view，只取昨天的数据；拉链表建立view，只取结束时间为2099-99-99的数据。

## 动态build cube

```bash
#!/bin/bash
cube_name=order_cube
do_date=`date -d '-1 day' +%F`

#获取00:00时间戳
start_date_unix=`date -d "$do_date 00:00:00" +%s`
start_date=$(($start_date_unix*1000))

#获取24:00的时间戳
stop_date=$(($start_date+86400000))

curl -X PUT -H "Authorization: Basic QURNSU46S1lMSU4=" -H 'Content-Type: application/json' -d '{"startTime":'$start_date', "endTime":'$stop_date', "buildType":"BUILD"}' http://hadoop102:7070/kylin/api/cubes/$cube_name/build

```

## 调优

#### 使用衍生维度  

衍生维度用于在有效维度内将维度表上的非主键维度排除掉，并使用维度表的主键（其实是事实表上相应的外键）来替代它们。Kylin会在底层记录维度表主键与维度表其他维度之间的映射关系，以便在查询时能够动态地将维度表的主键“翻译”成这些非主键维度，并进行实时聚合。

但是，有以下两个原则需要遵守：

1.如果某个维度表只选了一个维度，那么该维度不要选做衍生维度。

2.如果一个维度的重复度很高（去重前的数据量/基数），那么该维度不要选做衍生维度。一般的，重复度超过 10就视为重复度很高。

#### 使用聚合组

聚合组（Aggregation Group）是一种强大的剪枝工具。聚合组假设一个Cube的所有维度均可以根据业务需求划分成若干组（当然也可以是一个组），由于同一个组内的维度更可能同时被同一个查询用到，因此会表现出更加紧密的内在关联。每个分组的维度集合均是Cube所有维度的一个子集，不同的分组各自拥有一套维度集合，它们可能与其他分组有相同的维度，也可能没有相同的维度。每个分组各自独立地根据自身的规则贡献出一批需要被物化的Cuboid，所有分组贡献的Cuboid的并集就成为了当前Cube中所有需要物化的Cuboid的集合。不同的分组有可能会贡献出相同的Cuboid，构建引擎会察觉到这点，并且保证每一个Cuboid无论在多少个分组中出现，它都只会被物化一次。

对于每个分组内部的维度，用户可以使用如下三种可选的方式定义，它们之间的关系，具体如下。

1）强制维度（Mandatory），如果一个维度被定义为强制维度，那么这个分组产生的所有Cuboid中每一个Cuboid都会包含该维度。每个分组中都可以有0个、1个或多个强制维度。如果根据这个分组的业务逻辑，则相关的查询一定会在过滤条件或分组条件中，因此可以在该分组中把该维度设置为强制维度。举例：

![1656061647530](C:\Users\neu_hgx\AppData\Roaming\Typora\typora-user-images\1656061647530.png)

2）层级维度（Hierarchy），每个层级包含两个或更多个维度。假设一个层级中包含D1，D2…Dn这n个维度，那么在该分组产生的任何Cuboid中， 这n个维度只会以（），（D1），（D1，D2）…（D1，D2…Dn）这n+1种形式中的一种出现。每个分组中可以有0个、1个或多个层级，不同的层级之间不应当有共享的维度。如果根据这个分组的业务逻辑，则多个维度直接存在层级关系，因此可以在该分组中把这些维度设置为层级维度。举例：

![1656061710695](C:\Users\neu_hgx\AppData\Roaming\Typora\typora-user-images\1656061710695.png)

3）联合维度（Joint），每个联合中包含两个或更多个维度，如果某些列形成一个联合，那么在该分组产生的任何Cuboid中，这些联合维度要么一起出现，要么都不出现。每个分组中可以有0个或多个联合，但是不同的联合之间不应当有共享的维度（否则它们可以合并成一个联合）。如果根据这个分组的业务逻辑，多个维度在查询中总是同时出现，则可以在该分组中把这些维度设置为联合维度。举例：

![1656061780928](C:\Users\neu_hgx\AppData\Roaming\Typora\typora-user-images\1656061780928.png)

聚合组的设计非常灵活，甚至可以用来描述一些极端的设计。假设我们的业务需求非常单一，只需要某些特定的Cuboid，那么可以创建多个聚合组，每个聚合组代表一个Cuboid。具体的方法是在聚合组中先包含某个Cuboid所需的所有维度，然后把这些维度都设置为强制维度。这样当前的聚合组就只能产生我们想要的那一个Cuboid了。

再比如，有的时候我们的Cube中有一些基数非常大的维度，如果不做特殊处理，它就会和其他的维度进行各种组合，从而产生一大堆包含它的Cuboid。包含高基数维度的Cuboid在行数和体积上往往非常庞大，这会导致整个Cube的膨胀率变大。如果根据业务需求知道这个高基数的维度只会与若干个维度（而不是所有维度）同时被查询到，那么就可以通过聚合组对这个高基数维度做一定的“隔离”。我们把这个高基数的维度放入一个单独的聚合组，再把所有可能会与这个高基数维度一起被查询到的其他维度也放进来。这样，这个高基数的维度就被“隔离”在一个聚合组中了，所有不会与它一起被查询到的维度都没有和它一起出现在任何一个分组中，因此也就不会有多余的Cuboid产生。这点也大大减少了包含该高基数维度的Cuboid的数量，可以有效地控制Cube的膨胀率。

#### RowKey优化

**1.被用作过滤的维度放在前边。**

**2.基数大的维度放在基数小的维度前边。**  
