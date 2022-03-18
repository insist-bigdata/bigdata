package top.damoncai.rtdw.utils

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core._
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, TermsQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder
import java.util

object ESUtil {

  private var factory:JestClientFactory = null;

  def getClient:JestClient = {
    if(factory == null) build();
    factory.getObject;
  }

  def build(): Unit = {
    factory=new JestClientFactory
    factory.setHttpClientConfig(
      new HttpClientConfig.Builder("http://ha01:9200" )
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(1000).
      build()
    )
  }

  /**
   * 插入数据
   */
  def put(id: String): Unit = {
    val jestClient: JestClient = getClient

    val source =
      """
        |{
        |"id":400,
        |"name":"incident red sea",
        |"doubanScore":5.0,
        |"actorList":[
        |{"id":4,"name":"zhang cuishan"}
        |]
        |}
        |""".stripMargin

    val index:Index = new Index.Builder(source)
      .index("index_test")
      .`type`("movie")
      .id(id)
      .build()

    jestClient.execute(index)
    jestClient.close()
  }

  /**
   * 查询单条数据
   */
    def selectOne(id: String): Unit = {
      val jestClient: JestClient = getClient

      val get: Get = new Get.Builder("index_test", id).build()

      val result: DocumentResult = jestClient.execute(get)
      println(result.getJsonString)
      jestClient.close()
    }

  /**
   * 多条件查询一
   */
    def mutiSearchOne: Unit = {
      val jestClient: JestClient = getClient

      val query:String =
        """
          |{
          |  "query": {
          |    "match_all": {}
          |  }
          |}
          |""".stripMargin

      val search: Search = new Search.Builder(query)
        .addIndex("index_test")
        .build()

      val result: SearchResult = jestClient.execute(search)
      import java.util
      val list: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])
      import scala.collection.JavaConverters._
      val list1: List[util.Map[String, Any]] = list.asScala.map(_.source).toList
      println(list1.mkString("\n"))
      jestClient.close()
    }

  /**
   * 多条件查询二
   */
  def mutiSearchTwo: Unit = {
    val jestClient: JestClient = getClient

    val searchSourceBuilder:SearchSourceBuilder  = new SearchSourceBuilder
    val boolQueryBuilder:BoolQueryBuilder = new BoolQueryBuilder()
    boolQueryBuilder.must(new MatchQueryBuilder("name","天龙"))
    boolQueryBuilder.filter(new TermsQueryBuilder("actorList.name.keyword","李若单"))
    searchSourceBuilder.query(boolQueryBuilder)
    searchSourceBuilder.from(0)
    searchSourceBuilder.size(10)
    searchSourceBuilder.sort("doubanScore",SortOrder.ASC)
    searchSourceBuilder.highlighter(new HighlightBuilder().field("name"))
    val query:String = searchSourceBuilder.toString()
    val search: Search = new Search.Builder(query)
      .addIndex("index_test")
      .build()

    val result: SearchResult = jestClient.execute(search)
    import java.util
    val list: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])
    import scala.collection.JavaConverters._
    val list1: List[util.Map[String, Any]] = list.asScala.map(_.source).toList
    println(list1.mkString("\n"))
    jestClient.close()
  }

  //向 ES 中批量插入数据
  //参数 1：批量操作的数据 参数 2：索引名称
  def bulkInsert(sourceList:List[(String,Any)],indexName:String): Unit = {
    println("len:" + sourceList.size)
    if (sourceList != null && sourceList.size > 0) {
      //获取操作对象
      val jest: JestClient = getClient
      //构造批次操作
      val bulkBuild: Bulk.Builder = new Bulk.Builder
      //对批量操作的数据进行遍历
      for ((id,source) <- sourceList) {
        val index: Index = new Index.Builder(source)
          .index(indexName)
          .id(id)
          .`type`("_doc")
          .build()
        //将每条数据添加到批量操作中
        bulkBuild.addAction(index)
      }
      //Bulk 是 Action 的实现类，主要实现批量操作
      val bulk: Bulk = bulkBuild.build()
      //执行批量操作 获取执行结果
      val result: BulkResult = jest.execute(bulk)
      //通过执行结果 获取批量插入的数据
      val items: util.List[BulkResult#BulkResultItem] = result.getItems
      println("保存到 ES" + items.size() + "条数")
      //关闭连接
      jest.close()
    }
  }

  def main(args: Array[String]): Unit = {
//    put("2")
    selectOne("1")
//    mutiSearchTwo
  }

}
