package com.phq.elasticsearchdemo.es_hrset_client;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class InitElasticSearch {
   
	//elasticsearch 初始化
	public static RestHighLevelClient  getClient() {
		HttpHost https  = new HttpHost("192.168.1.130",9200,"http");
		RestHighLevelClient client =new  RestHighLevelClient(RestClient.builder(https));
		return client;
	}
}
