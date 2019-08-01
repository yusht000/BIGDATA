package com.flink.process.CEP

import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object EventPatter {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    // 抽取事件时间
    val loginEventStream: DataStream[LoginEvent] = env.fromCollection(List(
      LoginEvent("1", "192.168.0.1", "fail", "1558430842"),
      LoginEvent("1", "192.168.0.2", "fail", "1558430843"),
      LoginEvent("1", "192.168.0.3", "fail", "1558430844"),
      LoginEvent("2", "192.168.10.10", "success", "1558430845")
    )).assignAscendingTimestamps(_.eventTime.toLong * 1000)


    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType.equals("fail"))
      .next("next")
      .where(_.eventType.equals("fail"))
      .within(Time.seconds(10))


    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)

    val loginEventFailDataStream: DataStream[(String, String, String)] = patternStream.select(
      pattern => {
        val first: LoginEvent = pattern.getOrElse("begin", null).iterator.next()
        val second: LoginEvent = pattern.getOrElse("next", null).iterator.next()
        (second.userId, second.ip, second.eventType)
      }
    )

    loginEventFailDataStream.print()

    env.execute("Flink Streaming Scala API Skeleton")
  }

  case class LoginEvent(userId: String, ip: String, eventType: String, eventTime: String)

}
