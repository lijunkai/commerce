import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object PageStat {

  def main(args: Array[String]): Unit = {
    // filter params
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParm = JSONObject.fromObject(jsonStr)
    val taskUUID = UUID.randomUUID().toString

    // spark sql session
    val sparkConf = new SparkConf().setAppName("sessionStat").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    import sparkSession.implicits._

    // 1.init config
    // 1.1 get business page_page
    val targetPageFlows = taskParm.getString(Constants.PARAM_TARGET_PAGE_FLOW).split(",")
    val page_pageList = targetPageFlows.slice(0, targetPageFlows.length - 1).zip(targetPageFlows.tail).map { case (page1, page2) =>
      page1 + "_" + page2
    }

    // 2.action page data [(page1_page2,100),(page2_page3,200)]
    val sessionId2ActionRDD = sparkSession.sql(s"SELECT * FROM user_visit_action WHERE date >= '${taskParm.getString(Constants.PARAM_START_DATE)}' AND date <= '${taskParm.getString(Constants.PARAM_END_DATE)}'")
      .as[UserVisitAction].rdd.map(item => (item.session_id, item))
    // 2.1 group by sessionId； one session in a group
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()
    // 2.2 flatMap one -> many  session has many page_page
    val page_page2count = sessionId2GroupRDD.flatMap { case (_, actionObjects) =>
      // sort by actionTime
      val list = actionObjects.toList.sortBy(_.action_time)
      // zip [page1_page2,10]
      // filter no in config page_pages
      val pageTuple = list.slice(0, list.length - 1).zip(list.tail)
      pageTuple.map { case (action1, action2) =>
        (action1.page_id + "_" + action2.page_id, 1)
      }.filter { case (page_page, _) =>
        page_pageList.contains(page_page)
      }
    }

    // 1.2 filter first page count
    var lastPageCount = sessionId2ActionRDD.filter(_._2.page_id == targetPageFlows(0).toLong).count()

    // 2.3 [(page1_page2,1),(page2_page3,1)] get page_page Map
    val page_page2CountMap = page_page2count.countByKey()

    val list = for (page_page <- page_pageList) yield {
      page_page2CountMap.get(page_page) match {
        case Some(count) =>
          val retust = count.toDouble / lastPageCount
          lastPageCount = count
          page_page + "=" + retust
      }
    }

    // 2.4 write to mysql
    sparkSession.sparkContext
      .makeRDD(List(PageSplitConvertRate(taskUUID, list.mkString("|"))))
      .toDF().write
      .format("jdbc")
      .mode(SaveMode.Append)
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "page_split_convert_rate")
      .save()
  }

}
