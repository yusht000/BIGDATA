package com.flink

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object StreamingJob_Elasticsearch {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val inputDataStream: DataStream[String] = env.fromCollection(List(
      "a",
      "b"
    ))

    val httpHosts = new util.ArrayList[HttpHost]()

    httpHosts.add(new HttpHost("hadoop102", 9200, "http"))

    val esSink: ElasticsearchSink.Builder[String] = new ElasticsearchSink.Builder[String](
      httpHosts,
      new ElasticsearchSinkFunction[String] {


        def createIndexRequest(element: String): IndexRequest = {

          val jsonMap = new util.HashMap[String, String]()

          jsonMap.put("data", element)

          Requests.indexRequest()
            .index("my_index")
            .`type`("_doc")
            .source(jsonMap)
        }

        override def process(
                              element: String,
                              runtimeContext: RuntimeContext,
                              requestIndexer: RequestIndexer
                            ): Unit = {
          requestIndexer.add(createIndexRequest(element))
        }
      }
    )

    inputDataStream.addSink(esSink.build)
    env.execute("Flink Streaming Scala API Skeleton")
  }
}
