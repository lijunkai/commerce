import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * 会话步长,时长统计项累加器
  *
  * @author liangchuanchuan
  */
class SessionStatAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

  /**
    * 步长,时长统计项
    */
  val map = new mutable.HashMap[String, Int] {


  }

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAcc = new SessionStatAccumulator
    map.synchronized {
      newAcc.map ++= map
    }
    newAcc
  }

  override def reset(): Unit = map.clear()

  override def add(v: String): Unit = map.put(v, map.getOrElse(v, 0) + 1)

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = other match {
    case o: SessionStatAccumulator =>
      for ((key, value) <- o.map) {
        map.put(key, map.getOrElse(key, 0) + value)
      }
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def value: mutable.HashMap[String, Int] = map
}
