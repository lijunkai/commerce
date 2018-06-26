#session需求
### 各个范围Session步长、访问时长占比统计
* 根据查询参数获取user_visit_action [sessionId:infos[]]
* 根据sessionId将user_visit_action聚合为一条信息 [sessionId:info]
* inner join user_info获取关联用户信息 [userId:(infoAgg,userInfo)]
* 转换key为sessionId [sessionId: (infoAgg,fullInfo)] cache
* 累加器实现 HashMap<步长_步数/时长_范围:String,次数:Integer>
* 行动操作后获取累加器中的数据并写入mysql

### session数据抽取
* 思考过程
    * 在一个时间范围内对数据进行抽取, 抽取100条
    * 确认最小粒度:每小时数据
    * 100条数据对应每小时应该抽取的量,小时在当天占的比例*每天应该抽取的量
    * 确认每小时抽取量后,获取每小时数据量随机值比如
        * 【2018-06-01 12】一共有10条数据
        * 确认【2018-06-01 12】要抽取比例 抽取2条数据
        * 在10条数据范围内随机且不重复获取数据索引下标
    * 根据索引下标获取小时数据
        * 小时数据进行groupByKey
        * 小时数据 toList hourList
        * 遍历索引下标List for(index<-list){hourList.get(index)}
        * 数据装载到一个List[抽取数据类]容器中
    * 压平小时数据为一个 List[抽取数据类]
* 实现思路
    * 根据全量信息 转换key为小时 [datehour:fullInfo]
    * 获取每小时的数据 countByKey Map(datehour,count)
    * 构建 天,小时->count的数据结构  Map(date,Map(hour,count))
    * 计算每天抽取抽取百分比 100 / session数据天数
    * 计算每小时抽取session count => (hourCount.toDouble / dayCount) * 天抽取比例
    * 根据每天抽取百分比,当天count,每小时count 构建每个小时随机抽取index Map(date,Map(hour,List(randomIndex)) )
    * 遍历全量数据，根据每小时抽取索引获取数据
    * 写入mysql