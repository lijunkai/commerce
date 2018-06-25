import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

/**
  * 会话相关需求
  *
  * @author liangchuanchuan
  */
object SessionStat {

  def main(args: Array[String]): Unit = {
    // filter params
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParm = JSONObject.fromObject(jsonStr)
    val taskUUID = UUID.randomUUID().toString

    // spark sql session
    val sparkConf = new SparkConf().setAppName("sessionStat").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    import sparkSession.implicits._

    // 1 common data
    // 1.1 table user_visit_action
    val sessionId2ActionRDD = sparkSession.sql(s"SELECT * FROM user_visit_action WHERE date >= '${taskParm.getString(Constants.PARAM_START_DATE)}' AND date <= '${taskParm.getString(Constants.PARAM_END_DATE)}'")
      .as[UserVisitAction].rdd.map(item => (item.session_id, item))
    val userId2ActionRDD = groupBySession(sessionId2ActionRDD)
    // 1.2 table user_info
    val userRDD = sparkSession.sql("SELECT * FROM user_info").as[UserInfo].rdd.map(user => (user.user_id, user))
    val userId2ActionRDDAndUserRDD = userId2ActionRDD.join(userRDD)
    // 1.3 full info [sessionId:(sessionInfo,userInfo)]
    val sessionId2FullInfo = fullInfoMap(userId2ActionRDDAndUserRDD)

    // 2 business1: session sept/time rate
    // 2.1 session accumulator
    val sessionStatisticAccumulator = new SessionStatAccumulator
    sparkSession.sparkContext.register(sessionStatisticAccumulator)
    // 2.2 filter by param and put accumulator
    val sessionId2Filter = getFilterRDD(sparkSession, taskParm, sessionStatisticAccumulator, sessionId2FullInfo)
    // 2.3 write to mysql
    sessionId2Filter.count()
    writeSessionRatioToMysql(sparkSession, taskUUID, sessionStatisticAccumulator.value)
  }

  /**
    * 计算累加器中的百分比写到mysql
    *
    * @param sparkSession
    * @param taskUUID
    * @param value
    */
  def writeSessionRatioToMysql(sparkSession: SparkSession, taskUUID: String,
                               value: mutable.HashMap[String, Int]): Unit = {
    // 1.取出累加器中的数据
    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    // 2.统计比例数据
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    // 3.比例数据封装为对象
    val stat = SessionAggrStat(taskUUID, session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    // 4.转换为dataFrame
    import sparkSession.implicits._
    val statDataFrame = sparkSession.sparkContext.makeRDD(Array(stat)).toDF()
    // 5.输出到mysql
    statDataFrame.write
      .format("jdbc")
      .mode(SaveMode.Append)
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_stat")
      .save()
  }

  /**
    * 过滤符合条件的rdd
    *
    * @param sparkSession
    * @param taskParam
    * @param sessionStatisticAccumulator
    * @param sessionId2FullInfoRDD
    * @return
    */
  def getFilterRDD(sparkSession: SparkSession,
                   taskParam: JSONObject,
                   sessionStatisticAccumulator: SessionStatAccumulator,
                   sessionId2FullInfoRDD: RDD[(String, String)]) = {
    // 限制条件
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val searchKeywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val clickCategories = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    // 限制条件信息
    var filterInfo = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (searchKeywords != null) Constants.PARAM_KEYWORDS + "=" + searchKeywords + "|" else "") +
      (if (clickCategories != null) Constants.PARAM_CATEGORY_IDS + "=" + clickCategories else "")
    if (filterInfo.endsWith("\\|")) {
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)
    }

    // 过滤不符合条件的数据,符合过滤条件的数据加入累加器
    sessionId2FullInfoRDD.filter { case (sessionId, fullInfo) =>
      var success = true
      if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
        success = false
      }
      if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS)) {
        success = false
      }
      if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES)) {
        success = false
      }
      if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX)) {
        success = false
      }
      if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS))
        success = false
      if (!ValidUtils.in(fullInfo, Constants.FIELD_CATEGORY_ID, filterInfo, Constants.PARAM_CATEGORY_IDS))
        success = false

      if (success) {
        def calculateVisitLength(visitLength: Long) = {
          if (visitLength >= 1 && visitLength <= 3) {
            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1s_3s)
          } else if (visitLength >= 4 && visitLength <= 6) {
            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_4s_6s)
          } else if (visitLength >= 7 && visitLength <= 9) {
            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_7s_9s)
          } else if (visitLength >= 10 && visitLength <= 30) {
            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10s_30s)
          } else if (visitLength > 30 && visitLength <= 60) {
            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30s_60s)
          } else if (visitLength > 60 && visitLength <= 180) {
            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1m_3m)
          } else if (visitLength > 180 && visitLength <= 600) {
            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_3m_10m)
          } else if (visitLength > 600 && visitLength <= 1800) {
            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10m_30m)
          } else if (visitLength > 1800) {
            sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30m)
          }
        }

        def calculateStepLength(stepLength: Long): Unit = {
          if (stepLength >= 1 && stepLength <= 3) {
            sessionStatisticAccumulator.add(Constants.STEP_PERIOD_1_3)
          } else if (stepLength >= 4 && stepLength <= 6) {
            sessionStatisticAccumulator.add(Constants.STEP_PERIOD_4_6)
          } else if (stepLength >= 7 && stepLength <= 9) {
            sessionStatisticAccumulator.add(Constants.STEP_PERIOD_7_9)
          } else if (stepLength >= 10 && stepLength <= 30) {
            sessionStatisticAccumulator.add(Constants.STEP_PERIOD_10_30)
          } else if (stepLength > 30 && stepLength <= 60) {
            sessionStatisticAccumulator.add(Constants.STEP_PERIOD_30_60)
          } else if (stepLength > 60) {
            sessionStatisticAccumulator.add(Constants.STEP_PERIOD_60)
          }
        }

        val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
        val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong

        sessionStatisticAccumulator.add(Constants.SESSION_COUNT)
        calculateStepLength(stepLength)
        calculateVisitLength(visitLength)
      }
      success
    }
  }

  /**
    * 根据sessionId进行分组 [userId:actionInfo]
    *
    * @param actionRDD
    */
  def groupBySession(actionRDD: RDD[(String, UserVisitAction)]) = {
    // [sessionId:item[]]
    val sessionId2GroupActionRDD = actionRDD.groupByKey()
    sessionId2GroupActionRDD.cache()

    // [userId:itemStr]
    sessionId2GroupActionRDD.map { case (sessionId, items) =>
      var startTime: Date = null
      var endTime: Date = null
      val searchKeywords = new StringBuffer("")
      val clickCategries = new StringBuffer("")
      var userId = -1L
      var stepLength = 0L

      for (item <- items) {
        if (userId == -1L) {
          userId = item.user_id
        }

        // session 开始时间,结束时间
        val action_time = DateUtils.parseTime(item.action_time)
        if (startTime == null || startTime.after(action_time)) {
          startTime = action_time
        }
        if (endTime == null || endTime.before(action_time)) {
          endTime = action_time
        }

        // session 搜索词/点击类别id 去重
        val search_keyword = item.search_keyword
        if (search_keyword != null && !searchKeywords.toString.contains(search_keyword)) {
          searchKeywords.append(search_keyword).append(",")
        }
        val click_category_id = item.click_category_id
        if (click_category_id != -1 && !clickCategries.toString.contains(click_category_id)) {
          clickCategries.append(click_category_id).append(",")
        }
        stepLength += 1
      }

      // 获取访问时长(s)
      val visitLength = (endTime.getTime - startTime.getTime) / 1000
      // 搜索词
      val searchKW = StringUtils.trimComma(searchKeywords.toString)
      // 点击类别id
      val clickCG = StringUtils.trimComma(clickCategries.toString)

      // 字段名=字段值|字段名=字段值|
      val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
        Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKW + "|" +
        Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCG + "|" +
        Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
        Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
        Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)
      (userId, aggrInfo)
    }

  }

  /**
    * session和用户信息合并
    *
    * @param userId2ActionRDDAndUserRDD
    * @return
    */
  def fullInfoMap(userId2ActionRDDAndUserRDD: RDD[(Long, (String, UserInfo))]) = {
    userId2ActionRDDAndUserRDD.map { case (userId, (actionInfo, userInfo)) =>
      val sessionId = StringUtils.getFieldFromConcatString(actionInfo, "\\|", Constants.FIELD_SESSION_ID)
      val fullInfo = actionInfo + "|" + Constants.FIELD_AGE + "=" + userInfo.age + "|" +
        Constants.FIELD_PROFESSIONAL + "=" + userInfo.professional + "|" +
        Constants.FIELD_SEX + "=" + userInfo.sex + "|" +
        Constants.FIELD_CITY + "=" + userInfo.city

      (sessionId, fullInfo)
    }
  }


}
