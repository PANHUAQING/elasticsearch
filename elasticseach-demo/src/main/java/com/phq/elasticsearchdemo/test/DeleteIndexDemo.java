package com.phq.elasticsearchdemo.test;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;

import com.phq.elasticsearchdemo.es_hrset_client.InitElasticSearch;

public class DeleteIndexDemo {
     //删除索引
	@Test
	public void  deleteIndex() {
		
		RestHighLevelClient client = InitElasticSearch.getClient();
		
		DeleteIndexRequest  request =  new DeleteIndexRequest();
		
	}
}
