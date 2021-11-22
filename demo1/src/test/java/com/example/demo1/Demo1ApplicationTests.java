package com.example.demo1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.catalina.User;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.PatternMatchUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@SpringBootTest
@RunWith(SpringRunner.class)
class Demo1ApplicationTests {

    @Autowired
    private RestHighLevelClient client;

    String index ="time";

    /**
     * 创建索引
     */
    @Test
    public void createIndex() {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
        try {
            CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            System.out.println(createIndexResponse.isAcknowledged());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断索引是否存在
     */
    @Test
    public void indexIsExit() {
//        String index = "222";
        GetIndexRequest getIndexRequest = new GetIndexRequest(index);
        try {
            boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
            System.err.println(exists);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 添加文档
    @Test
    public void createDoc() {
        UserDemo user = new UserDemo("张三", "男", null);
        IndexRequest indexRequest = new IndexRequest(index);
        indexRequest.source(JSONObject.toJSONString(user), XContentType.JSON);
        try {
            IndexResponse index = client.index(indexRequest, RequestOptions.DEFAULT);
            System.err.println(index.getResult());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    // 批量添加文档
    @Test
    public void bulkDoc() {
        List<IndexRequest> indexRequests = new ArrayList<>();
        indexRequests.add(list("lisi", "女"));
        indexRequests.add(list("王武", "男"));
        indexRequests.add(list("赵六", "男"));
        BulkRequest request = new BulkRequest();
        for (IndexRequest indexRequest : indexRequests) {
            request.add(indexRequest);
        }
        try {
            BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
            System.err.println(JSONObject.toJSONString(response));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public IndexRequest list(String name, String sex) {
        IndexRequest indexRequests = new IndexRequest(index);
        indexRequests.source(JSONObject.toJSONString(new UserDemo(name,sex, null)), XContentType.JSON);
        return indexRequests;
    }

    /**
     * 获取索引内的所有文档
     */
    @Test
    public void getIndex() {
        try {
            SearchRequest searchRequest = new SearchRequest(index);
            SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
            RestStatus restStatus = search.status();
            if (restStatus == RestStatus.OK) {
                List<UserDemo> userDemos = new ArrayList<>();
                SearchHits hits = search.getHits();
                hits.forEach(item -> userDemos.add(JSON.parseObject(item.getSourceAsString(), UserDemo.class)));
                System.err.println("=-=-=-=-"+userDemos);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 分页获取索引中的文档
     */
    @Test
    public void getDocByPage() {
        try {
            SearchRequest searchRequest = new SearchRequest(index);

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
            searchSourceBuilder.query(matchAllQueryBuilder);
/*
            QueryBuilders.termQuery(“key”, “vaule”); // 完全匹配
            QueryBuilders.termsQuery(“key”, “vaule1”, “vaule2”) ; //一次匹配多个值
            QueryBuilders.matchQuery(“key”, “vaule”) //单个匹配, field不支持通配符, 前缀具高级特性
            QueryBuilders.multiMatchQuery(“text”, “field1”, “field2”); //匹配多个字段, field有通配符忒行
            QueryBuilders.matchAllQuery(); // 匹配所有文件
*/

            MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("name", "张")
                    .fuzziness(Fuzziness.AUTO) //模糊查询
                    .prefixLength(3)    // 在匹配查询上设置前缀长度选项,指明区分词项的共同前缀长度，默认是0
                    .maxExpansions(10);//设置最大扩展选项以控制查询的模糊过程

            //查询条件 添加，name = 张三
            //sourceBuilder.query(QueryBuilders.termQuery("user", "kimchy"));
            searchSourceBuilder.query(matchQueryBuilder);
            // 分页
            searchSourceBuilder.from(0);
            searchSourceBuilder.size(2);
//            searchSourceBuilder.sort(new FieldSortBuilder("age").order(SortOrder.ASC));

            searchRequest.source(searchSourceBuilder);

            SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
            RestStatus restStatus = search.status();
            if (restStatus == RestStatus.OK) {
                List<UserDemo> userDemos = new ArrayList<>();
                SearchHits hits = search.getHits();
                hits.forEach(item -> userDemos.add(JSON.parseObject(item.getSourceAsString(), UserDemo.class)));
                System.err.println("=-=-=-=-"+userDemos);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 查询结果高亮
    @Test
    public void highLight() {
        String highligtFiled = "name";
        QueryBuilder queryBuilder = QueryBuilders.matchQuery(highligtFiled, "张三");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder);
        //设置高亮
        String preTags = "<strong>";
        String postTags = "</strong>";
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags(preTags);
        highlightBuilder.postTags(postTags);
        highlightBuilder.field(highligtFiled);
        searchSourceBuilder.highlighter(highlightBuilder);
        SearchRequest request = new SearchRequest(index);
        request.source(searchSourceBuilder);
        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            RestStatus restStatus = response.status();
            if (restStatus == RestStatus.OK) {
                List<UserDemo> userDemos = new ArrayList<>();
                response.getHits().forEach(x -> {
                    UserDemo userDemo = JSON.parseObject(x.getSourceAsString(), UserDemo.class);
                    userDemo.setHighlightFieldMap(x.getHighlightFields());
                    userDemos.add(userDemo);
                });
                System.err.println(userDemos);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void test() {
        List<String> authorities = new ArrayList<>();
        authorities.add("11");
        authorities.add("2");
        authorities.add("3");
        String a = "3";
        System.err.println(PatternMatchUtils.simpleMatch(authorities.get(0), "11"));
/*        authorities.stream.filter(x -> {
            return PatternMatchUtils.simpleMatch(x, "3");
        });*/
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class UserDemo {
        private String name;
        private String sex;
        private Map<String, HighlightField> highlightFieldMap;
    }

}
