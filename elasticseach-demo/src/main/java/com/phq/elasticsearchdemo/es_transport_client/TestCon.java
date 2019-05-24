package com.phq.elasticsearchdemo.es_transport_client;

import java.net.InetAddress;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class TestCon {

    private static String host="192.168.1.130"; 
    private static int port=9300; 
    
    public static final String CLUSTER_NAME="elasticsearch-phq"; 
    
    private static Settings.Builder settings=Settings.builder().put("cluster.name",CLUSTER_NAME);
	
	public static void main(String[] args) throws Exception{
		@SuppressWarnings({ "resource", "unchecked" })
		TransportClient client = new PreBuiltTransportClient(settings.build())
                .addTransportAddress(new TransportAddress(InetAddress.getByName(host),port));
		System.out.println(client);
		client.close();
	}
}
