/**
  * 根据类别id属性值进行比较
  *
  * @param click 点击次数
  * @param order 订单次数
  * @param pay   支付次数
  * @author liangchuanchuan
  */
case class CategoryOrder(click: Long, order: Long, pay: Long) extends Ordered[CategoryOrder] {

  /**
    * 比较
    *
    * @param that
    * @return
    */
  override def compare(that: CategoryOrder): Int = {
    if (click.compare(that.click) != 0) {
      return click.compare(that.click)
    } else if (order.compare(that.order) != 0) {
      return order.compare(that.order)
    } else if (pay.compare(that.pay) != 0) {
      return pay.compare(that.pay)
    }
    0
  }

}
