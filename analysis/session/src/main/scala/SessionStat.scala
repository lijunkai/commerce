import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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

    // spark sql session
    val sparkConf = new SparkConf().setAppName("sessionStat").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    import sparkSession.implicits._

    // 1.common data

    // 1.1 sql select user_visit_action table to rdd
    val actionRDD = sparkSession.sql(s"SELECT * FROM user_visit_action WHERE date >= '${taskParm.getString(Constants.PARAM_START_DATE)}' AND date <= '${taskParm.getString(Constants.PARAM_END_DATE)}'")
      .as[UserVisitAction].rdd

    // 1.2 group by session [session:info]

    // 1.3 join by user_id [user: (sessionInfo,userInfo)]

    // 1.4 session map [session:(sessionInfo,userInfo)]

    // 2 business requirement: session sept rate and time rate

    // 2.1 session accumulator

    // 2.2 executor put different session sept/time in accumulator

    // 2.3 driver merge and compute sept/time rate

    // 2.3 write to mysql

    actionRDD.foreach(println(_))
  }


}
