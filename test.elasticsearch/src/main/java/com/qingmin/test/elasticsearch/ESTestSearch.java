package com.qingmin.test.elasticsearch;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ESTestSearch {

	TransportClient transportClient = new TransportClient();
	String index = "score";
	//String type = "score-1";
	String type = "score-3";
	
	@Before
	public void connection() throws Exception{//为了用本地环境，我固定写死了地址，所以这个地址做了修改，这里连接是主节点，但是我查数据是从副本节点读取的
		transportClient.addTransportAddress(new InetSocketTransportAddress("192.168.1.101",9300));
		
	}
	
	/**
	 * @删除 索引库
	 * 
	 */
	@Ignore
	public void test1() throws Exception{//这里是删除指定索引库，慎用！就不测试了，我还需要这些数据
		//DeleteIndexResponse response = transportClient.admin().indices().prepareDelete("testsettings").get();
		DeleteResponse response = transportClient.prepareDelete(index, type, "5").get();//删除一条数据
	}
	
	/**
	 * @初始化数据就是 添加数据
	 */
	@Ignore
	public void test2() throws Exception{
		
		XContentBuilder builder = XContentFactory.jsonBuilder()
				.startObject()
				.field("name","hehe")
				.field("score",50)
				.endObject();
		IndexResponse response = transportClient.prepareIndex(index, type ,"5").setSource(builder).get();
		System.out.println(response.getVersion());
		
	}
	
	/**
	 * @使用不同的分片查询方式,这里我做了实验，用本地比用随机模式确实快，本地模式需要172毫秒，随机模式是保证在集群中找到不重复的分片就可以了，这样如果一个集群就装了所有分片的信息，它还是会在集群其他节点找，是会慢下来的，需要207毫秒
	 * 本地：138   6
	 * 随机：130   6
	 * 主分片：156  6
	 * 优先主分片，其次是副分片 151 6 这种是比较 合理的
	 */
	@Test
	public void test3() throws Exception{
		System.out.println("start:"+System.currentTimeMillis());
		SearchResponse searchResponse = transportClient.prepareSearch(index)//这类如果不加任何setPreference就是随机找分片，这样随机找会实现负载均衡
				//.setPreference("_local")//优先本地查找，只是效率比较高，这样本地节点压力大
				//.setPreference("_primary")//只在主分片中找,因为主分片的数据是最全的，因为副本可能还没有做好
				//.setPreference("_primary_first")
				//.setPreference("_only_node:W_93k1SlSmebtyN4-Vi5_w")//这个节点id可以在bigdesk里面查到，按理说我这里面取出来应该不是6条了，因为我节点少只有两个，做不出这样的效果
				//.setPreference("_prefer_node:W_93k1SlSmebtyN4-Vi5_w")
				.setPreference("_shards:2,3")//这种数据最不全
				.setTypes(type)
				.setFrom(0)//这里的from代表的就是从查询的结果列表中从默认从角标为0的开始要，因为我们查询后符合条件的数据每次总有一天，所以只能从0开始才有数据，写1的话就没数据了
				.setSize(10)//这里的from代表的就是从查询的结果列表中从默认取10条，类似于limit 10的功能
				.setExplain(true)//查询的结果跟搜索关键字越相似的来排序
				.get();
		
		SearchHits hits = searchResponse.getHits();  //这里返回的是一个接口，而非一个对象，还不能理解或者直接按照一个对象列表来用，这个就是符合查询条件（这个条件仅仅是查询结果之前的条件）的数据
		long totalHits = hits.getTotalHits();
		System.out.println("test15 response totalHits:"+totalHits);
		
		SearchHit[] hits2 = hits.getHits();//The hits of the search request (based on the search type, and from / size provided).  这个就是符合查询条件，并且符合查询结果的筛选条件的数据，所以就会出现符合查询条件的是1条，但是筛选完之后肯那个是0条
		for(SearchHit searchHit : hits2){
			System.out.println("test15 response searchHit:"+searchHit.getSourceAsString());
		}
		System.out.println("end:"+System.currentTimeMillis());
	}
	
	/**
	 * @往指定分片 添加数据
	 */
	@Ignore
	public void test4() throws Exception{
		
		XContentBuilder builder = XContentFactory.jsonBuilder()
				.startObject()
				.field("name","lily")
				.field("score",80)
				.endObject();
		IndexResponse response = transportClient.prepareIndex(index, type ,"8").setRouting("test").setSource(builder).get();//這裡可以通過源碼hash方法測試算得test的hash值是2
		System.out.println(response.getVersion());
		
	}
	
}
