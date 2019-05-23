package com.phq.elasticsearchdemo.test;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.Test;

import com.phq.elasticsearchdemo.es_hrset_client.InitElasticSearch;

public class GetDocumentDemo {

	@Test
	public void  getDocument() {
		try {
		RestHighLevelClient client = InitElasticSearch.getClient();
		
		 // 1、创建获取文档请求
        GetRequest request = new GetRequest(
                "mess",   //索引
                "_doc",   // mapping type
                "1");    
        
      //request.routing("routing");
        //request.version(2);
        
        //request.fetchSourceContext(new FetchSourceContext(false)); //是否获取_source字段
        //选择返回的字段
        String[] includes = new String[]{"message", "*Date"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fetchSourceContext); 
        
        //也可写成这样
        /*String[] includes = Strings.EMPTY_ARRAY;
        String[] excludes = new String[]{"message"};
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fetchSourceContext);*/
        
        
        // 取stored字段
        /*request.storedFields("message"); 
        GetResponse getResponse = client.get(request);
        String message = getResponse.getField("message").getValue();*/
        
        
        //3、发送请求        
        GetResponse getResponse = null;
        try {
            // 同步请求
            getResponse = client.get(request);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
            	System.out.println("没有找到该id的文档" );
            }
            if (e.status() == RestStatus.CONFLICT) {
            	System.out.println("获取时版本冲突了，请在此写冲突处理逻辑！" );
            }
            System.out.println("获取文档异常"+e);
        }
        
        //4、处理响应
        if(getResponse != null) {
            String index = getResponse.getIndex();
            String type = getResponse.getType();
            String id = getResponse.getId();
            if (getResponse.isExists()) { // 文档存在
                long version = getResponse.getVersion();
                String sourceAsString = getResponse.getSourceAsString(); //结果取成 String       
                Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();  // 结果取成Map
                byte[] sourceAsBytes = getResponse.getSourceAsBytes();    //结果取成字节数组
                
                System.out.println("index:" + index + "  type:" + type + "  id:" + id);
                System.out.println(sourceAsString);
                
            } else {
            	System.out.println("没有找到该id的文档" );
            }
        }
        
        
        //异步方式发送获取文档请求
        /*
        ActionListener<GetResponse> listener = new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                
            }
        
            @Override
            public void onFailure(Exception e) {
                
            }
        };
        client.getAsync(request, listener);
        */
        
    } catch (IOException e) {
        e.printStackTrace();
    }
	}
}
