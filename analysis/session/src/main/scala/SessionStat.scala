import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SessionStat {

  def main(args: Array[String]): Unit = {

    def main(args: Array[String]): Unit = {

      // 首先，读取我们的任务限制条件
      val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
      val taskParm = JSONObject.fromObject(jsonStr)

      // 创建UUID
      val taskUUID = UUID.randomUUID().toString

      // 创建sparkConf
      val sparkConf = new SparkConf().setAppName("sessionStat").setMaster("local[*]")

      // 创建sparkSession
      val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

      // 获取动作表里的初始数据
      // UserVisitAction(2018-06-23,80,f14889e03ff94f7fbfda6ab113570a0d,8,2018-06-23 13:43:46,null,-1,-1,81,13,null,null,3)
      val actionRDD = getBasicActionData(sparkSession, taskParm)

      actionRDD.foreach(println(_))
    }

    def getBasicActionData(sparkSession: SparkSession, taskParm: JSONObject) = {
      val startDate = ParamUtils.getParam(taskParm, Constants.PARAM_START_DATE)
      val endDate = ParamUtils.getParam(taskParm, Constants.PARAM_END_DATE)

      val sql = s"select * from user_visit_action where date>='$startDate' and date<='$endDate'"
      // DataFrame -> DataSet -> RDD[UserVisitAction]
      sparkSession.sql(sql)
        .as[UserVisitAction]
        .rdd
    }

  }

}
