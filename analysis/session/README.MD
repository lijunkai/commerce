#session需求
### 各个范围Session步长、访问时长占比统计
* 根据查询参数获取user_visit_action [sessionId:infos[]]
* 根据sessionId将user_visit_action聚合为一条信息 [sessionId:info]
* inner join user_info获取关联用户信息 [userId:(infoAgg,userInfo)]
* 转换key为sessionId [sessionId: (infoAgg,userInfo)] cache
* 累加器实现 HashMap<步长_步数/时长_范围:String,次数:Integer>
* 行动操作后获取累加器中的数据并写入mysql
