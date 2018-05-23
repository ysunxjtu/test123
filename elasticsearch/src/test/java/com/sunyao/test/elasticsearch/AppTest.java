package com.sunyao.test.elasticsearch;

import com.google.gson.JsonObject;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.index.query.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    private Logger logger = LoggerFactory.getLogger(AppTest.class);

    public final static String HOST = "192.168.27.128";

    public final static int PORT = 9300;//http请求的端口是9200，客户端是9300

    private TransportClient client = null;

    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        assertTrue( true );
    }

    @Before
    public void getConnect() throws UnknownHostException{
        client = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddresses(
                new InetSocketTransportAddress(InetAddress.getByName(HOST),PORT));
        logger.info("连接信息：" + client.toString());
    }

    @After
    public void close(){
        if(null != client){
            client.close();
        }
    }

    /***
     * 创建索引库
     * 需求:创建一个索引库为：msg消息队列,类型为：tweet,id为1
     * 索引库的名称必须为小写
     * @throws IOException
     */
    @Test
    public void createIndex1() throws IOException{
        IndexResponse response = client.prepareIndex("msg","tweet","1").setSource(XContentFactory.jsonBuilder()
        .startObject().field("username","张三")
        .field("sendDate",new Date())
        .field("msg","你好李四")
        .endObject()).get();

        //logger.info("索引名称：" + response.getIndex() + "\n类型：" + response.getType()
         //  + "\n文档ID:" + response.getId() + "\n当前实例状态：" + response.status());

        System.out.println("索引名称：" + response.getIndex() + ", 类型：" + response.getType()
                 + " ,文档ID:" + response.getId() + " ,当前实例状态：" + response.status());
    }

    /***
     * 创建索引-传入json字符串
     * @throws IOException
     */
    @Test
    public void createIndex2() throws IOException{
          String jsonStr = "{" +
                  "\"userName\":\"张三\"," +
                  "\"sendDate\":\"2017-11-30\"," +
                  "\"msg\":\"你好李四\"" +
                  "}";
          IndexResponse response = client.prepareIndex("weixin","tweet").setSource(jsonStr, XContentType.JSON).get();

          System.out.println("索引名称：" + response.getIndex() + ", 类型：" + response.getType()
                + " ,文档ID:" + response.getId() + " ,当前实例状态：" + response.status());

    }

    /***
     * 创建索引-传入Map对象
     * @throws IOException
     */
    @Test
    public void createIndex3() throws  IOException{
        Map<String,Object> map = new HashMap<String,Object>();
        map.put("userName","张三");
        map.put("sendDate",new Date());
        map.put("msg","你好李四");

        IndexResponse response = client.prepareIndex("momo","tweet").setSource(map).get();
        System.out.println("索引名称：" + response.getIndex() + ", 类型：" + response.getType()
                + " ,文档ID:" + response.getId() + " ,当前实例状态：" + response.status());
    }

    /***
     * 创建索引-传入json对象
     * @throws IOException
     */
    @Test
    public void createIndex4() throws IOException{
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("userName","张三");
        jsonObject.addProperty("sendDate","2017-02-30");
        jsonObject.addProperty("msg","你好李四");

        IndexResponse response = client.prepareIndex("qq","tweet").setSource(jsonObject,XContentType.JSON).get();
        System.out.println("索引名称：" + response.getIndex() + ", 类型：" + response.getType()
                + " ,文档ID:" + response.getId() + " ,当前实例状态：" + response.status());
    }

    /***
     * 创建索引-传入javabean
     * @throws Exception
     */
    @Test
    public void createIndex5() throws IOException{
        Student stu = new Student();
        stu.setAge(20);
        stu.setName("张三");

        JsonObject jsonObject = new JsonObject();

       // ObjectMapper mapper = new ObjectMapper();
       // IndexResponse response = client.prepareIndex("msg","tweet","2");

    }

    /**
     * 从索引库获取数据
     */
    @Test
    public void getData1(){
        GetResponse getResponse = client.prepareGet("msg","tweet","10").get();
        System.out.println("index:" + getResponse.getIndex() + ",type:" + getResponse.getType() + ",id:" + getResponse.getId() +
                ",username:" + getResponse.getField("username") + ",resoource:" + getResponse.getSourceAsMap() + ",version" + getResponse.getVersion());
        System.out.println(getResponse.getSourceAsString());
    }

    /***
     * 更新索引库数据
     */
    @Test
    public void updateData(){
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("userName", "王五");
        jsonObject.addProperty("sendDate", "2008-08-08");
        jsonObject.addProperty("msg","你好,张三，好久不见");

        UpdateResponse response = client.prepareUpdate("msg","tweet","1")
                .setDoc(jsonObject.toString(),XContentType.JSON).get();
        System.out.println("索引名称：" + response.getIndex() + ", 类型：" + response.getType()
                + " ,文档ID:" + response.getId() + " ,当前实例状态：" + response.status());
    }
    /***
     * 更新索引库数据
     */
    @Test
    public void updateData2(){
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("username", "jack");
        jsonObject.addProperty("sendDate", "2010-08-08");
        jsonObject.addProperty("msg","helloworld");

        UpdateResponse response = client.prepareUpdate("msg","tweet","10")
                .setDoc(jsonObject.toString(),XContentType.JSON).get();
        System.out.println("索引名称：" + response.getIndex() + ", 类型：" + response.getType()
                + " ,文档ID:" + response.getId() + " ,当前实例状态：" + response.status());
    }

    /***
     * 删除索引库数据
     * 删除索引库，不可逆慎用
     */
    @Test
    public void deleteData(){
        DeleteResponse response = client.prepareDelete("msg","tweet","1").get();
        System.out.println("索引名称：" + response.getIndex() + ", 类型：" + response.getType()
                + " ,文档ID:" + response.getId() + " ,当前实例状态：" + response.status());
    }

    /***
     * 获取索引节点总数
     */
    @Test
    public void getCount(){

    }

    /**
     * 通过prepareBulk执行批处理
     *
     * @throws IOException
     */
    @Test
    public void testBulk() throws IOException
    {
        //1:生成bulk
        BulkRequestBuilder bulk = client.prepareBulk();

        //2:新增
        IndexRequest add = new IndexRequest("msg", "tweet", "10");
        add.source(XContentFactory.jsonBuilder()
                .startObject()
                .field("name", "Henrry").field("age", 30)
                .endObject());

        //3:删除
        DeleteRequest del = new DeleteRequest("msg", "tweet", "10");

        //4:修改
        XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("name", "jack_1").field("age", 19).endObject();
        UpdateRequest update = new UpdateRequest("msg", "tweet", "10");
        update.doc(source);

        bulk.add(del);
        bulk.add(add);
        bulk.add(update);
        //5:执行批处理
        BulkResponse bulkResponse = bulk.get();
        if(bulkResponse.hasFailures())
        {
            BulkItemResponse[] items = bulkResponse.getItems();
            for(BulkItemResponse item : items)
            {
                System.out.println(item.getFailureMessage());
            }
        }
        else
        {
            System.out.println("全部执行成功！");
        }
    }

    /**
     * 通过prepareSearch查询索引库
     * setQuery(QueryBuilders.matchQuery("name", "jack"))
     * setSearchType(SearchType.QUERY_THEN_FETCH)
     *
     */
    @Test
    public void testSearch()
    {
        SearchResponse searchResponse = client.prepareSearch("msg")
                .setTypes("tweet")
                .setQuery(QueryBuilders.matchAllQuery()) //查询所有
                //.setQuery(QueryBuilders.matchQuery("name", "tom").operator(Operator.AND)) //根据tom分词查询name,默认or
                //.setQuery(QueryBuilders.multiMatchQuery("tom", "name", "age")) //指定查询的字段
                //.setQuery(QueryBuilders.queryString("name:to* AND age:[0 TO 19]")) //根据条件查询,支持通配符大于等于0小于等于19
                //.setQuery(QueryBuilders.termQuery("name", "tom"))//查询时不分词
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setFrom(0).setSize(10)//分页
                .addSort("age", SortOrder.DESC)//排序
                .get();

        SearchHits hits = searchResponse.getHits();
        long total = hits.getTotalHits();
        System.out.println(total);
        SearchHit[] searchHits = hits.hits();
        for(SearchHit s : searchHits)
        {
            System.out.println(s.getSourceAsString());
        }
    }

    /**
     * 多索引，多类型查询
     * timeout
     */
    @Test
    public void testSearchsAndTimeout()
    {
        TimeValue timeValue = new TimeValue(1000);
        SearchResponse searchResponse = client.prepareSearch("msg","weixin").setTypes("tweet","tweet")
                .setQuery(QueryBuilders.matchAllQuery())
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setTimeout(timeValue)
                .get();

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println(totalHits);
        SearchHit[] hits2 = hits.getHits();
        for(SearchHit h : hits2)
        {
            System.out.println(h.getSourceAsString());
        }
    }

    /**
     * 使用QueryBuilder
     * termQuery("key", obj) 完全匹配
     * termsQuery("key", obj1, obj2..)   一次匹配多个值
     * matchQuery("key", Obj) 单个匹配, field不支持通配符, 前缀具高级特性
     * multiMatchQuery("text", "field1", "field2"..);  匹配多个字段, field有通配符忒行
     * matchAllQuery();         匹配所有文件
     */
    @Test
    public void testQueryBuilder() {
       //  QueryBuilder queryBuilder = QueryBuilders.termQuery("username", "jack");
        //QueryBuilders.termsQuery("user", new ArrayList<String>().add("kimchy"));
          QueryBuilder queryBuilder = QueryBuilders.matchQuery("name", "jack_1");  //完全匹配
//        QueryBuilder queryBuilder = QueryBuilders.multiMatchQuery("kimchy", "user", "message", "gender");

        //  QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
          searchFunction(queryBuilder);
    }


    /**
     * 查询遍历抽取
     * @param queryBuilder
     */
    private void searchFunction(QueryBuilder queryBuilder) {

    /*    SearchResponse response = client.prepareSearch("msg","weixin").setTypes("tweet","tweet")
                .setQuery(QueryBuilders.matchAllQuery())
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setTimeout(new TimeValue(1000))
                .get();*/

        SearchResponse response = client.prepareSearch("msg")
                .setTypes("tweet")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setScroll(new TimeValue(1000))
                .setQuery(queryBuilder)
                .setSize(10).get();

       // while(true) {
            for (SearchHit hit : response.getHits()) {
                Iterator<Map.Entry<String, Object>> iterator = hit.getSource().entrySet().iterator();
                while(iterator.hasNext()) {
                    Map.Entry<String, Object> next = iterator.next();
                    System.out.println(next.getKey() + ": " + next.getValue());
                    if(response.getHits().hits().length == 0) {
                        break;
                    }
                }
            }

         //  testResponse(response);
          //  break;
      //  }
//        testResponse(response);
    }

    /**
     * 组合查询
     * must(QueryBuilders) :   AND
     * mustNot(QueryBuilders): NOT
     * should:                  : OR
     */
    @Test
    public void testQueryBuilder2() {
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("sendDate", "2017-11-30"));
               // .must(QueryBuilders.termQuery("sendDate", "2017-11-30"))
                //.should(QueryBuilders.termQuery("msg", "你好李四"));
        searchFunction(queryBuilder);
    }

    /**
     * 只查询一个id的
     * QueryBuilders.idsQuery(String...type).ids(Collection<String> ids)
     */
    @Test
    public void testIdsQuery() {
        QueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("1");
        searchFunction(queryBuilder);
    }

    /**
     * 包裹查询, 高于设定分数, 不计算相关性
     */
    @Test
    public void testConstantScoreQuery() {
        QueryBuilder queryBuilder = QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("name", "jack_1")).boost(2.0f);
        searchFunction(queryBuilder);
        // 过滤查询
//        QueryBuilders.constantScoreQuery(FilterBuilders.termQuery("name", "kimchy")).boost(2.0f);

    }

    /**
     * disMax查询
     * 对子查询的结果做union, score沿用子查询score的最大值,
     * 广泛用于muti-field查询
     */
    @Test
    public void testDisMaxQuery() {
        QueryBuilder queryBuilder = QueryBuilders.disMaxQuery()
                .add(QueryBuilders.termQuery("name", "jack_1"))  // 查询条件
                .add(QueryBuilders.termQuery("message", "hello"))
                .boost(1.3f)
                .tieBreaker(0.7f);
        searchFunction(queryBuilder);
    }

    /**
     * 模糊查询
     * 不能用通配符, 不知道干啥用
     */
    @Test
    public void testFuzzyQuery() {
        QueryBuilder queryBuilder = QueryBuilders.fuzzyQuery("name", "jack_1");  //模糊查询结果不正确，原因未知
        searchFunction(queryBuilder);
    }

    /**
     * 父或子的文档查询
     */
    @Test
    public void testChildQuery() {
      //  QueryBuilder queryBuilder = QueryBuilders.geoHashCellQuery("sonDoc", QueryBuilders.termQuery("name", "vini"));
      //  searchFunction(queryBuilder);
    }

    /**
     * moreLikeThisQuery: 实现基于内容推荐, 支持实现一句话相似文章查询
     * {
     "more_like_this" : {
     "fields" : ["title", "content"],   // 要匹配的字段, 不填默认_all
     "like_text" : "text like this one",   // 匹配的文本
     }
     }

     percent_terms_to_match：匹配项（term）的百分比，默认是0.3

     min_term_freq：一篇文档中一个词语至少出现次数，小于这个值的词将被忽略，默认是2

     max_query_terms：一条查询语句中允许最多查询词语的个数，默认是25

     stop_words：设置停止词，匹配时会忽略停止词

     min_doc_freq：一个词语最少在多少篇文档中出现，小于这个值的词会将被忽略，默认是无限制

     max_doc_freq：一个词语最多在多少篇文档中出现，大于这个值的词会将被忽略，默认是无限制

     min_word_len：最小的词语长度，默认是0

     max_word_len：最多的词语长度，默认无限制

     boost_terms：设置词语权重，默认是1

     boost：设置查询权重，默认是1

     analyzer：设置使用的分词器，默认是使用该字段指定的分词器
     */
    @Test
    public void testMoreLikeThisQuery() {
        QueryBuilder queryBuilder = QueryBuilders.moreLikeThisQuery(new String[]{"jack_1","name"});
//                            .minTermFreq(1)         //最少出现的次数
//                            .maxQueryTerms(12);        // 最多允许查询的词语
        searchFunction(queryBuilder);
    }

    /**
     * 前缀查询
     */
    @Test
    public void testPrefixQuery() {
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("username", "jack");
        searchFunction(queryBuilder);
    }

    /**
     * 查询解析查询字符串
     */
    @Test
    public void testQueryString() {
        QueryBuilder queryBuilder = QueryBuilders.queryStringQuery("jack hello");
        searchFunction(queryBuilder);
    }

    /**
     * 范围内查询
     */
    @Test
    public void testRangeQuery() {
        QueryBuilder queryBuilder = QueryBuilders.rangeQuery("username")
                .from("jack")
                .to("张三")
                .includeLower(true)     // 包含上界
                .includeUpper(true);      // 包含下届
        searchFunction(queryBuilder);
    }

    /**
     * 跨度查询
     */
    @Test
    public void testSpanQueries() {
        QueryBuilder queryBuilder1 = QueryBuilders.spanFirstQuery(QueryBuilders.spanTermQuery("name", "葫芦580娃"), 30000);     // Max查询范围的结束位置

/*
        QueryBuilder queryBuilder2 = QueryBuilders.spanNearQuery()
                .clause(QueryBuilders.spanTermQuery("name", "葫芦580娃")) // Span Term Queries
                .clause(QueryBuilders.spanTermQuery("name", "葫芦3812娃"))
                .clause(QueryBuilders.spanTermQuery("name", "葫芦7139娃"))
                .slop(30000)                                               // Slop factor
                .inOrder(false)
                .collectPayloads(false);

        // Span Not
        QueryBuilder queryBuilder3 = QueryBuilders.spanNotQuery()
                .include(QueryBuilders.spanTermQuery("name", "葫芦580娃"))
                .exclude(QueryBuilders.spanTermQuery("home", "山西省太原市2552街道"));

        // Span Or
        QueryBuilder queryBuilder4 = QueryBuilders.spanOrQuery()
                .clause(QueryBuilders.spanTermQuery("name", "葫芦580娃"))
                .clause(QueryBuilders.spanTermQuery("name", "葫芦3812娃"))
                .clause(QueryBuilders.spanTermQuery("name", "葫芦7139娃"));
*/

        // Span Term
        QueryBuilder queryBuilder5 = QueryBuilders.spanTermQuery("name", "葫芦580娃");
    }

    /**
     * 测试子查询
     */
    @Test
    public void testTopChildrenQuery() {
      /*  QueryBuilders.hasChildQuery("tweet",
                QueryBuilders.termQuery("user", "kimchy"))
                .scoreMode("max");*/
    }

    /**
     * 通配符查询, 支持 *
     * 匹配任何字符序列, 包括空
     * 避免* 开始, 会检索大量内容造成效率缓慢
     */
    @Test
    public void testWildCardQuery() {
        QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("name", "jack*");
        searchFunction(queryBuilder);
    }

    /**
     * 嵌套查询, 内嵌文档查询
     */
    @Test
    public void testNestedQuery() {
     /*   QueryBuilder queryBuilder = QueryBuilders.nestedQuery("location",
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchQuery("location.lat", 0.962590433140581))
                        .must(QueryBuilders.rangeQuery("location.lon").lt(36.0000).gt(0.000)))
                .scoreMode("total");*/
    }

    /**
     * 测试索引查询
     */
    @Test
    public void testIndicesQueryBuilder () {
        QueryBuilder queryBuilder = QueryBuilders.indicesQuery(
                QueryBuilders.termQuery("user", "kimchy"), "index1", "index2")
                .noMatchQuery(QueryBuilders.termQuery("user", "kimchy"));

    }

    /**
     * 对结果设置高亮显示
     */
    public void testHighLighted() {
        /*  5.0 版本后的高亮设置
         * client.#().#().highlighter(hBuilder).execute().actionGet();
        HighlightBuilder hBuilder = new HighlightBuilder();
        hBuilder.preTags("<h2>");
        hBuilder.postTags("</h2>");
        hBuilder.field("user");        // 设置高亮显示的字段
        */
        // 加入查询中
      /*  SearchResponse response = client.prepareSearch("blog")
                .setQuery(QueryBuilders.matchAllQuery())
                .addHighlightedField("user")        // 添加高亮的字段
                .setHighlighterPreTags("<h1>")
                .setHighlighterPostTags("</h1>")
                .execute().actionGet();*/

        // 遍历结果, 获取高亮片段
 /*       SearchHits searchHits = response.getHits();
        for(SearchHit hit:searchHits){
            System.out.println("String方式打印文档搜索内容:");
            System.out.println(hit.getSourceAsString());
            System.out.println("Map方式打印高亮内容");
            System.out.println(hit.getHighlightFields());

            System.out.println("遍历高亮集合，打印高亮片段:");
            FieldStats.Text[] text = hit.getHighlightFields().get("title").getFragments();
            for (FieldStats.Text str : text) {
                System.out.println(str.string());
            }
        }*/
    }

    /**
     * 对response结果的分析
     * @param response
     */
    public void testResponse(SearchResponse response) {
        // 命中的记录数
        long totalHits = response.getHits().totalHits();

        for (SearchHit searchHit : response.getHits()) {
            // 打分
            float score = searchHit.getScore();
            // 文章id
            int id = Integer.parseInt(searchHit.getSource().get("id").toString());
            // title
            String title = searchHit.getSource().get("title").toString();
            // 内容
            String content = searchHit.getSource().get("content").toString();
            // 文章更新时间
            long updatetime = Long.parseLong(searchHit.getSource().get("updatetime").toString());
        }
    }
}
