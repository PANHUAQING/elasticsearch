package com.phq.elasticsearchdemo.es_transport_client;

import java.net.InetAddress;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class TestSearch {

	private static String host="192.168.1.130"; 
	private static int port=9300; 
    
    public static final String CLUSTER_NAME="elasticsearch-phq";
    
    private static Settings.Builder settings=Settings.builder().put("cluster.name",CLUSTER_NAME);
    
    private TransportClient client=null;
    
    @SuppressWarnings({ "resource", "unchecked" })
	@Before
    public void getCient()throws Exception{
    	client = new PreBuiltTransportClient(settings.build())
                .addTransportAddress(new TransportAddress(InetAddress.getByName(host),port));
    }
    
    @After
    public void close(){
    	if(client!=null){
    		client.close();
    	}
    }
    
    /**
     * �������� ����ĵ�
     * @throws Exception
     */
    @Test
    public void testIndex()throws Exception{
    	JsonArray jsonArray=new JsonArray();
    	
    	JsonObject jsonObject=new JsonObject();
    	jsonObject.addProperty("title", "ǰ��3���ټ�ǰ��");
    	jsonObject.addProperty("publishDate", "2017-12-29");
    	jsonObject.addProperty("content", "һ�Ժû������ƣ����� �Σ�����ɣ�֣�� �Σ���Ů�Ѷ���Ϊһ��С��������֣����ҡ��ܾ���أ������ϴ���������ҹ�ꡢ�ɶ��뽻������Ϸŷ������ڶ�����������ף���ƽ����ڡ����Ӷ�������һϵ�к�Ц�Ĺ��¡�������Ů��ͬ�ʹ���ȴ���ӡ�����֮�����������Ů�����������మ��ɱ�޾��ڡ�Ȼ����ʵ�ġ�������ȴ����⧲�������һ������������������һ�Ծ������ȫ�������������˶���������յ�ѡ�����ٴ������������Ҳ������");
    	jsonObject.addProperty("director", "������");
    	jsonObject.addProperty("price", "35");
    	jsonArray.add(jsonObject);
    	
    	
    	JsonObject jsonObject2=new JsonObject();
    	jsonObject2.addProperty("title", "����֮Ѫ");
    	jsonObject2.addProperty("publishDate", "2017-12-29");
    	jsonObject2.addProperty("content", "2007�꣬Dr.James�ڰ뵺�����̵�֧�����о������ˡ��о������У������˰����ҷ�������ͻ���ɱ�䣬���뵺������ɱ��������������֯���ӹ������˵��о���Dr.James����������ֻ��Ѱ�󾯷��ı������ع��ֶ������� �Σ��������뿪������Σ��СŮ������������֤�˱�������...ʮ�����һ���ƻ�С˵������֮Ѫ���ĳ��������˺�����������֯�����غ�����ɭ����־�� �Σ�����ɱ���İ뵺�����̵Ķ��ӣ����Լ������������ֶ���������������ʼ�ӽ�һ������ͨ��Ů��Nancy��ŷ������ �Σ��������Ҫ�õ������ϵ����ܡ���������Ļ���������ض���İ�����Ҳ�ٴγ��֣��ڶ�β���֮������ץ��Nancy���ֶ�����ɭ�����ò�������һͬǰȥ��ȣ��ؼ�ʱ��ȴ������ɭ��Ȼ�Ǳ�ɱ���İ뵺�����̵Ķ��ӣ������˵�ʵ���¼Ҳ��������ɭ֮��......");
    	jsonObject2.addProperty("director", "������");
    	jsonObject2.addProperty("price", "45");
    	jsonArray.add(jsonObject2);
    	
    	JsonObject jsonObject3=new JsonObject();
    	jsonObject3.addProperty("title", "�����ս8�����ľ�����ʿ");
    	jsonObject3.addProperty("publishDate", "2018-01-05");
    	jsonObject3.addProperty("content", "�������ս�����ľ�����ʿ���н�ǰ���������ս��ԭ�����ѡ��ľ��飬������һ����ȫ����Ϯ֮�£��������������׵��� Daisy Ridley �Σ����Ҷ���Լ������Ү�� John Boyega �Σ���������Ĭ������˹���������� Oscar Isaac �Σ���λ�������Ǹ��Եľ� ���ð�չ��¡�ǰ���о���ǿ��ԭ������������Ѱ�����ӵľ��ش�ʦ¬�ˡ������ߣ���ˡ����׶� Mark Hamill �Σ����ں��ߵ�ָ���½���ԭ��ѵ�����Ҷ�������һ�����������ɵ�����Ϊ�������ò��´���Ӫ������Լ��Ĺ�ȥ��������Ĭ����Ҫ��Ӧ��սʿ������Ľ�ɫת������һ��������Ҳ������һЩѪ�Ľ�ѵ��");
    	jsonObject3.addProperty("director", "������Լ��ѷ");
    	jsonObject3.addProperty("price", "55");
    	jsonArray.add(jsonObject3);
    	
    	JsonObject jsonObject4=new JsonObject();
    	jsonObject4.addProperty("title", "���ߵ���ȭ");
    	jsonObject4.addProperty("publishDate", "2017-12-29");
    	jsonObject4.addProperty("content", "�����ȭ�����ӵİ����������� �Σ��������������ʮ�������������С������ �Σ���һ��ԩ�ң�û�뵽��Ϊһ������ĵ������Ů���廥�����Ա���Һ����˻��ӻ�����������ȭ̳�Ĵ����Ҳ�ҿ��˼�ȭ������ܣ�����һ���鷳�����������ڡ������š������������ǣ����� �Σ���ָ���£�����������������ߵ���ȭ��");
    	jsonObject4.addProperty("director", "���� / �ų���");
    	jsonObject4.addProperty("price", "35");
    	jsonArray.add(jsonObject4);
    	
    	JsonObject jsonObject5=new JsonObject();
    	jsonObject5.addProperty("title", "ս��2");
    	jsonObject5.addProperty("publishDate", "2017-07-27");
    	jsonObject5.addProperty("content", "���·����ڷ��޸����Ĵ��ϣ����˹���棨�⾩ �Σ�������������¬����������������������Ư��һ��������������������ô����ʱ��һ��ͻ��������������������ļƻ���ͻȻ��������һ�����޹������ң������԰�ȫ���룬ȴ���޷���������Ϊ���˵�ʹ���������ճ��������������������ɱ�е�ͬ��������չ���������������Ŷ����ĳ��������ڵ������𽥸��գ����չ�����ս������Ϊͬ����ս����");
    	jsonObject5.addProperty("director", "�⾩");
    	jsonObject5.addProperty("price", "38");
    	jsonArray.add(jsonObject5);
    	
    	for(int i=0;i<jsonArray.size();i++){
    		JsonObject jo=jsonArray.get(i).getAsJsonObject();
    		IndexResponse response=client.prepareIndex("film", "dongzuo")
    				.setSource(jo.toString(), XContentType.JSON).get();
    		System.out.println("�������ƣ�"+response.getIndex());
    		System.out.println("���ͣ�"+response.getType());
    		System.out.println("�ĵ�ID��"+response.getId());
    		System.out.println("��ǰʵ��״̬��"+response.status());
    	}
    }
    
    /**
     * ��ѯ����
     * @throws Exception
     */
    @Test
    public void searchAll()throws Exception{
    	SearchRequestBuilder srb=client.prepareSearch("film").setTypes("dongzuo");
    	SearchResponse sr=srb.setQuery(QueryBuilders.matchAllQuery())
    		.execute()
    		.actionGet(); // ��ѯ����
    	SearchHits hits=sr.getHits();
    	for(SearchHit hit:hits){
    		System.out.println(hit.getSourceAsString());
    	}
    }
    
    /**
     * ��ҳ��ѯ
     * @throws Exception
     */
    @Test
    public void searchaging()throws Exception{
    	SearchRequestBuilder srb=client.prepareSearch("film").setTypes("dongzuo");
    	SearchResponse sr=srb.setQuery(QueryBuilders.matchAllQuery())
    		.setFrom(1)
    		.setSize(3)
    		.execute()
    		.actionGet(); 
    	SearchHits hits=sr.getHits();
    	for(SearchHit hit:hits){
    		System.out.println(hit.getSourceAsString());
    	}
    }
    
    /**
     * �����ѯ
     * @throws Exception
     */
    @Test
    public void searchSort()throws Exception{
    	SearchRequestBuilder srb=client.prepareSearch("film").setTypes("dongzuo");
    	SearchResponse sr=srb.setQuery(QueryBuilders.matchAllQuery())
    		.addSort("publishDate", SortOrder.DESC)
    		.execute()
    		.actionGet(); 
    	SearchHits hits=sr.getHits();
    	for(SearchHit hit:hits){
    		System.out.println(hit.getSourceAsString());
    	}
    }
    
    /**
     * �����й���
     * @throws Exception
     */
    @Test
    public void searchInclude()throws Exception{
    	SearchRequestBuilder srb=client.prepareSearch("film").setTypes("dongzuo");
    	SearchResponse sr=srb.setQuery(QueryBuilders.matchAllQuery())
    		.setFetchSource(new String[]{"title","price"}, null)
    		.execute()
    		.actionGet(); 
    	SearchHits hits=sr.getHits();
    	for(SearchHit hit:hits){
    		System.out.println(hit.getSourceAsString());
    	}
    }
    
    /**
     * ������ѯ
     * @throws Exception
     */
    @Test
    public void searchByCondition()throws Exception{
    	SearchRequestBuilder srb=client.prepareSearch("film").setTypes("dongzuo");
    	SearchResponse sr=srb.setQuery(QueryBuilders.matchQuery("title", "ս"))
    		.setFetchSource(new String[]{"title","price"}, null)
    		.execute()
    		.actionGet(); 
    	SearchHits hits=sr.getHits();
    	for(SearchHit hit:hits){
    		System.out.println(hit.getSourceAsString());
    	}
    }
    
    /**
     * ������ѯ������ʾ
     * @throws Exception
     */
    @Test
    public void searchHighlight()throws Exception{
    	SearchRequestBuilder srb=client.prepareSearch("film").setTypes("dongzuo");
    	HighlightBuilder highlightBuilder=new HighlightBuilder();
    	highlightBuilder.preTags("<h2><font>");
    	highlightBuilder.postTags("</font></h2>");
    	highlightBuilder.field("title");
		SearchResponse sr=srb.setQuery(QueryBuilders.matchQuery("title", "ս"))
    		.highlighter(highlightBuilder)
    		.setFetchSource(new String[]{"title","price"}, null)
    		.execute()
    		.actionGet(); 
    	SearchHits hits=sr.getHits();
    	for(SearchHit hit:hits){
    		System.out.println(hit.getSourceAsString());
    		System.out.println(hit.getHighlightFields());
    	}
    }
    
}
