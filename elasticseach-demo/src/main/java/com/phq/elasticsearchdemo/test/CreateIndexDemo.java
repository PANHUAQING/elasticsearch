package com.phq.elasticsearchdemo.test;

import java.io.IOException;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.junit.jupiter.api.Test;

import com.phq.elasticsearchdemo.es_hrset_client.InitElasticSearch;

public class CreateIndexDemo {
	//创建索引
	@Test
	public void createIndex() {
	   try (RestHighLevelClient client = InitElasticSearch.getClient();) {

           // 1、创建 创建索引request 参数：索引名mess
           CreateIndexRequest request = new CreateIndexRequest("message");

           // 2、设置索引的settings
           request.settings(
			 Settings.builder().put("index.number_of_shards", 3) // 分片数
			                   .put("index.number_of_replicas", 2) // 副本数
			                   //.put("analysis.analyzer.default.tokenizer", "ik_smart") // 默认分词器
           );

           // 3、设置索引的mappings
           request.mapping("_doc",
                   "  {\n" +
                   "    \"_doc\": {\n" +
                   "      \"properties\": {\n" +
                   "        \"message\": {\n" +
                   "          \"type\": \"text\"\n" +
                   "        }\n" +
                   "      }\n" +
                   "    }\n" +
                   "  }",
                   XContentType.JSON);

           // 4、 设置索引的别名
           request.alias(new Alias("mmm"));

           // 5、 发送请求
           // 5.1 同步方式发送请求
           CreateIndexResponse createIndexResponse = client.indices().create(request);

           // 6、处理响应
           boolean acknowledged = createIndexResponse.isAcknowledged();
           boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
           System.out.println("acknowledged = " + acknowledged);
           System.out.println("shardsAcknowledged = " + shardsAcknowledged);

           // 5.1 异步方式发送请求
           /*ActionListener<CreateIndexResponse> listener = new ActionListener<CreateIndexResponse>() {
               @Override
               public void onResponse(
                       CreateIndexResponse createIndexResponse) {
                   // 6、处理响应
                   boolean acknowledged = createIndexResponse.isAcknowledged();
                   boolean shardsAcknowledged = createIndexResponse
                           .isShardsAcknowledged();
                   System.out.println("acknowledged = " + acknowledged);
                   System.out.println(
                           "shardsAcknowledged = " + shardsAcknowledged);
               }

               @Override
               public void onFailure(Exception e) {
                   System.out.println("创建索引异常：" + e.getMessage());
               }
           };

           client.indices().createAsync(request, listener);
           */
       } catch (IOException e) {
           e.printStackTrace();
       }
   }
	//往索引里增加数据
	@Test
	public void indexIntoData() {
		try {
			
		RestHighLevelClient client = InitElasticSearch.getClient();
		// 1、创建索引请求
        IndexRequest request = new IndexRequest("mess",   //索引
        	"_doc",     // mapping type
            "1");     //文档id  
        
        
       // 2、准备文档数据
        // 方式一：直接给JSON串
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"title\":\"测试\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        request.source(jsonString, XContentType.JSON); 
        
        // 方式二：以map对象来表示文档
        /*
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "kimchy");
        jsonMap.put("postDate", new Date());
        jsonMap.put("message", "trying out Elasticsearch");
        request.source(jsonMap); 
        */
        
        // 方式三：用XContentBuilder来构建文档
        /*
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("user", "kimchy");
            builder.field("postDate", new Date());
            builder.field("message", "trying out Elasticsearch");
        }
        builder.endObject();
        request.source(builder); 
        */
        
        // 方式四：直接用key-value对给出
        /*
        request.source("user", "kimchy",
                        "postDate", new Date(),
                        "message", "trying out Elasticsearch");
        */
        
        //3、其他的一些可选设置
        /*
        request.routing("routing");  //设置routing值
        request.timeout(TimeValue.timeValueSeconds(1));  //设置主分片等待时长
        request.setRefreshPolicy("wait_for");  //设置重刷新策略
        request.version(2);  //设置版本号
        request.opType(DocWriteRequest.OpType.CREATE);  //操作类别  
        */
        
        //4、发送请求
        IndexResponse indexResponse = null;
        try {
            // 同步方式
            indexResponse = client.index(request);            
        } catch(ElasticsearchException e) {
            // 捕获，并处理异常
            //判断是否版本冲突、create但文档已存在冲突
            if (e.status() == RestStatus.CONFLICT) {
                System.out.println("冲突了，请在此写冲突处理逻辑！\n" + e.getDetailedMessage());
            }
            System.out.println("索引异常"+e);
        }
        
        //5、处理响应
        if(indexResponse != null) {
            String index = indexResponse.getIndex();
            String type = indexResponse.getType();
            String id = indexResponse.getId();
            long version = indexResponse.getVersion();
            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                System.out.println("新增文档成功，处理逻辑代码写到这里。");
            } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                System.out.println("修改文档成功，处理逻辑代码写到这里。");
            }
            // 分片处理信息
            ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                
            }
            // 如果有分片副本失败，可以获得失败原因信息
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason(); 
                    System.out.println("副本失败原因：" + reason);
                }
            }
        }
        
        
        //异步方式发送索引请求
        /*ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                
            }

            @Override
            public void onFailure(Exception e) {
                
            }
        };
        client.indexAsync(request, listener);
        */
        
    } catch (IOException e) {
        e.printStackTrace();
    }
		
	}

}
