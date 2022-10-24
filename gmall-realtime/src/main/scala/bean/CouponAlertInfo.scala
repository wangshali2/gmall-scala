package bean

/**
  * @author wsl
  * @version 2021-01-11
  *          ES中可以直接用java的set，可以直接解析成kv结构。scala会被解析成字符串。
  */
case class CouponAlertInfo(mid: String,
                           uids: java.util.HashSet[String],
                           itemIds: java.util.HashSet[String],
                           events: java.util.List[String],
                           ts: Long)

