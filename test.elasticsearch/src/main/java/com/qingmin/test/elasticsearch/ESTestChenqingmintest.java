package com.qingmin.test.elasticsearch;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;

//import java.net.InetSocketAddress;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ESTestChenqingmintest {  //method description by hot key is ATL+SHIFT+J   这里面所有的get（）方法相当于scala里面的collect，执行的意思

//	public static void main(String[] args){
//		
//	}
	TransportClient transportClient = new TransportClient();
	String index = "chenqingmintest";
	String type = "test1";
	
	@Before
	public void connection() throws Exception{
		transportClient.addTransportAddress(new InetSocketTransportAddress("192.168.56.130",9300));
		
	}
	
	@Ignore
	public void test1() throws Exception{//这个是默认连es集群名称，也就是elasticsearch，如果集群形成被人为修改过，那这样连就有问题了
		TransportClient transportClient = new TransportClient();
		transportClient.addTransportAddress(new InetSocketTransportAddress("192.168.56.130",9300));
		String index = "stream-appdynamics-v4";
		String type = "metrics";
		GetResponse response = transportClient.prepareGet(index, type, "AVKbYg4cgMiLunjDHL3Q").get();
		System.out.println("test1 response:"+response.getSourceAsString());
		//TransportAddress
	}
	
	/**
	 * @throws Exception
	 */
	@Ignore
	public void test2() throws Exception{//这里面代码写死了，这样是不好的，因为可能节点会加或者删除
		Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name","elasticsearch").build();
		
		TransportClient transportClient = new TransportClient(settings);
		transportClient.addTransportAddress(new InetSocketTransportAddress("192.168.56.130",9300));
		String index = "stream-appdynamics-v4";
		String type = "metrics";
		String id = "AVKbYg4cgMiLunjDHL3Q";
		GetResponse response = transportClient.prepareGet(index, type, id).get();
		System.out.println("test2 response:"+response.getSourceAsString());
		//TransportAddress
	}
    
	/**
	 * @throws Exception
	 */
	/**
	 * @throws Exception
	 */
	@Ignore
	public void test3() throws Exception{
		Settings settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name","elasticsearch")
				.put("client.transport.sniff", true)//开启集群的嗅探功能，不管节点怎么变，只要集群正常运行，就可以把所有的节点搜到
				.build();
		
		TransportClient transportClient = new TransportClient(settings);
		transportClient.addTransportAddress(new InetSocketTransportAddress("192.168.56.130",9300));//这里一定要指定其中一个节点，这样才可以嗅探到其它节点
		
		ImmutableList<DiscoveryNode> connectedNodes = transportClient.connectedNodes();
		for(DiscoveryNode discoveryNode : connectedNodes){
			System.out.println("test3 hostname:"+discoveryNode.getHostName());
		}	
	}
		
	/**
	 * datasource json
	 */
	@Ignore
	public void test4() throws Exception{
		String id = "2";
		//String jsonstr="{\"date\":\"07\",\"weather\":\"sunny\",\"temp\":\"-5\",\"city\":\"urumqi\"}";
		String jsonstr="{\"date\":\"07\",\"weather\":\"cloudy\",\"temp\":\"15\",\"city\":\"shanghai\"}";
		//IndexResponse response = transportClient.prepareIndex(index, type, id).setSource(jsonstr).execute().actionGet();
		IndexResponse response = transportClient.prepareIndex(index, type, id).setSource(jsonstr).get();
		System.out.println("test4 response:"+response.getVersion());

	}
	
	/**
	 * datasource map
	 */
	@Ignore
	public void test5() throws Exception{
		String id = "3";
		HashMap<String,Object> hashMap = new HashMap<String,Object>();
		hashMap.put("date", "07");
		hashMap.put("weather", "cloudy");
		hashMap.put("temp", "0");
		hashMap.put("city", "beijing");
		hashMap.put("describe", "capital of China");
		//IndexResponse response = transportClient.prepareIndex(index, type, id).setSource(jsonstr).execute().actionGet();
		IndexResponse response = transportClient.prepareIndex(index, type, id).setSource(hashMap).get();
		System.out.println("test5 response:"+response.getVersion());

	}
	
	/**
	 * datasouce java bean   这种方式是最常规的
	 */
	@Ignore
	public void test6() throws Exception{
		String id = "4";
		Climate climate = new Climate();
		climate.setDate("07");
		climate.setCity("shenzhen");
		climate.setProvince("guangdong");
		climate.setTemp("20");
		climate.setWeather("rainny");
		ObjectMapper objectMapper = new ObjectMapper();
		//IndexResponse response = transportClient.prepareIndex(index, type, id).setSource(jsonstr).execute().actionGet();
		IndexResponse response = transportClient.prepareIndex(index, type, id).setSource(objectMapper.writeValueAsString(climate)).get();//这里不能直接把java bean对象解析
		System.out.println("test6 response:"+response.getVersion());//注意对于每一条数据都有对应的版本号，改一次就加一次

		

	}
	
	/**
	 * datasource es helpers   如果字段很多，可以for循环拼字段
	 */
	@Ignore
	public void test7() throws Exception{
		String id = "5";
		XContentBuilder xcontentbuilder = XContentFactory.jsonBuilder().startObject()
				.field("date", "07")
				.field("city", "tianjing")
				.field("temp","2")
				.field("date", "07")
				.field("weather", "foggy")
				.endObject();//这里似乎这个startObject相当于json字符串外面那个大括号
		//IndexResponse response = transportClient.prepareIndex(index, type, id).setSource(jsonstr).execute().actionGet();

		IndexResponse response = transportClient.prepareIndex(index, type, id).setSource(xcontentbuilder).get();//这里不能直接把java bean对象解析
		System.out.println("test7 response:"+response.getVersion());//注意对于每一条数据都有对应的版本号，改一次就加一次

	}
	
	/**
	 * query by ID
	 */
	@Ignore
	public void test8() throws Exception{
		String id = "5";
		//GetResponse response = transportClient.prepareGet(index, type, id).get();
		GetResponse response = transportClient.prepareGet(index, type, id).execute().actionGet();
		System.out.println("test8 response:"+response.getSourceAsString());//注意对于每一条数据都有对应的版本号，改一次就加一次

	}
	
	/**
	 * update by covering the old data 覆盖就是创建这条数据的时候，把已经有的id再重新创建一次就行了，这样就覆盖了，这里讲的是局部更新
	 */
	@Ignore
	public void test9() throws Exception{
		String id = "1";
		XContentBuilder xcontentbuilder = XContentFactory.jsonBuilder().startObject()
				.field("province", "xinjiang")
				.field("describe","the capital of xinjiang province")
				.endObject();
		//GetResponse response = transportClient.prepareGet(index, type, id).get();
		UpdateResponse response = transportClient.prepareUpdate(index, type, id).setDoc(xcontentbuilder).get();
		System.out.println("test9 response:"+response.getVersion());

	}
	
	/**
	 * update method 2
	 */
	@Ignore
	public void test10() throws Exception{
		String id = "4";
		XContentBuilder xcontentbuilder = XContentFactory.jsonBuilder().startObject()
				.field("describe","Special Economic Zone")
				.endObject();
		UpdateRequest request = new UpdateRequest(index,type,id);
		request.doc(xcontentbuilder);
		UpdateResponse response = transportClient.update(request).get();
		System.out.println("test10 response:"+response.getVersion());

	}
	
	/**
	 * update or insert 这个方法一定要注意
	 */
	@Ignore
	public void test11() throws Exception{
		
		String id = "8";
		XContentBuilder xcontentbuilder1 = XContentFactory.jsonBuilder().startObject()
				.field("city","chongqing")
				.endObject();
		XContentBuilder xcontentbuilder2 = XContentFactory.jsonBuilder().startObject()
				.field("describe","test describe")
				.field("date","09")
				.field("city","testcity")
				.field("temp","test temp")
				.field("weather","test weather")
				.endObject();
		UpdateRequest request = new UpdateRequest(index,type,id);
		request.doc(xcontentbuilder1);//新创建数据的时候，这一步还必须写，但是这个写上也没啥用，还会被后面的覆盖，这一行的作用就是如果6存在的话，就走这一步了，而下面的创建就不执行了
		request.upsert(xcontentbuilder2);
		UpdateResponse response = transportClient.update(request).get();
		System.out.println("test11 response:"+response.getVersion());

	}
	
	/**
	 * delete
	 */
	@Ignore
	public void test12() throws Exception{
		
		String id = "8";
		//DeleteResponse response = transportClient.prepareDelete(index, type, id).get();
		
		//下面这个明天看一下  这里可以指定索引库删除，也就是相当于删除索引库，这个方法还没有尝试成功
		DeleteByQueryRequestBuilder qb = transportClient.prepareDeleteByQuery(index).setTypes(type);
		qb.setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), FilterBuilders.idsFilter(id)));
		DeleteByQueryResponse response = transportClient.deleteByQuery(qb.request()).actionGet();
		System.out.println("test12 response:"+response.getIndex(id));

	}
	
	/**
	 * count  其实就是select count(*) from the table 
	 */
	@Ignore
	public void test13() throws Exception{
		CountResponse response = transportClient.prepareCount(index).get();
		System.out.println("test13 response:"+response.getCount());

	}
	
	/**
	 * bulk   批量操作
	 */
	@Ignore
	public void test14() throws Exception{
		XContentBuilder xcontentbuilder = XContentFactory.jsonBuilder().startObject()
				.field("describe","test describe")
				.field("date","09")
				.field("city","test city")
				.field("temp","test temp")
				.field("weather","test weather")
				.endObject();
		BulkRequestBuilder prepareBulk = transportClient.prepareBulk();
		String id = "10";
		IndexRequest indexRequest = new IndexRequest(index,type);
		indexRequest.id(id);
		indexRequest.source(xcontentbuilder);
		prepareBulk.add(indexRequest);
		for(int i=8;i<=10;i++){
			DeleteRequest deleteRequest = new DeleteRequest(index,type,String.valueOf(i));
			prepareBulk.add(deleteRequest);
		}
		
		BulkResponse bulkResponse = prepareBulk.get();
		if(bulkResponse.hasFailures()){
			BulkItemResponse[] bulkItemResponseList = bulkResponse.getItems();
			for(BulkItemResponse bulkItemResponse: bulkItemResponseList){
				System.err.println("test14 response failed:"+bulkItemResponse.getFailureMessage());
			}
		}else{
			System.out.println("test14 response success");
		}

	}
	
	/**
	 * query by other fields
	 * Pagination of results can be done by using the from and size parameters. The from parameter defines the offset from the first result you want to fetch. The size parameter allows you to configure the maximum amount of hits to be returned.
	 * gt大于
	 * gte大于等于
	 * lt小于
	 * lte小于等于
	 * 
	 * 
	 */
	@Ignore
	public void test15() throws Exception{//一定要注意数字类型是字符串还是int，否则排序排不出来
		SearchResponse searchResponse = transportClient.prepareSearch(index)
		.setTypes(type)
		.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)//这个是默认查询方式
		.addSort("city", SortOrder.ASC)//这个根据某一个字段排序
		//.setPostFilter(FilterBuilders.rangeFilter("city").from("a").to("m"))//这种过滤头和尾都包含了闭区间
		.setPostFilter(FilterBuilders.rangeFilter("city").gte("bf").lte("urumqi"))//这里的字符串比较有个问题什么叫大于，就是bf是大于beijing的，因为f大于e
		//.setQuery(QueryBuilders.matchQuery("city", "urumqi"))
		.setFrom(0)//这里的from代表的就是从查询的结果列表中从默认从角标为0的开始要，因为我们查询后符合条件的数据每次总有一天，所以只能从0开始才有数据，写1的话就没数据了
		//.setSize()//这里的from代表的就是从查询的结果列表中从默认取10条，类似于limit 10的功能
		.setExplain(true)//查询的结果跟搜索关键字越相似的来排序
		.get();
		SearchHits hits = searchResponse.getHits();  //这里返回的是一个接口，而非一个对象，还不能理解或者直接按照一个对象列表来用，这个就是符合查询条件（这个条件仅仅是查询结果之前的条件）的数据
		long totalHits = hits.getTotalHits();
		System.out.println("test15 response totalHits:"+totalHits);
		
		SearchHit[] hits2 = hits.getHits();//The hits of the search request (based on the search type, and from / size provided).  这个就是符合查询条件，并且符合查询结果的筛选条件的数据，所以就会出现符合查询条件的是1条，但是筛选完之后肯那个是0条
		for(SearchHit searchHit : hits2){
			System.out.println("test15 response searchHit:"+searchHit.getSourceAsString());
		}

	}
	
	/**
	 * @highlight
	 */
	@Test
	public void test17() throws Exception{//一定要注意数字类型是字符串还是int，否则排序排不出来
		SearchResponse searchResponse = transportClient.prepareSearch(index)
		.setTypes(type)
		.setQuery(QueryBuilders.matchQuery("city", "urumqi"))
		.addHighlightedField("city")
		.setHighlighterPreTags("<font color = 'red'>")
		.setHighlighterPostTags("</font>")
		//以上做完高亮集合数据结果集hi分开的，相当于看不到高亮的效果，需要自己弄在一起显示
		.setFrom(0)//这里的from代表的就是从查询的结果列表中从默认从角标为0的开始要，因为我们查询后符合条件的数据每次总有一天，所以只能从0开始才有数据，写1的话就没数据了
		//.setSize()//这里的from代表的就是从查询的结果列表中从默认取10条，类似于limit 10的功能
		.setExplain(true)//查询的结果跟搜索关键字越相似的来排序
		.get();
		SearchHits hits = searchResponse.getHits();  //这里返回的是一个接口，而非一个对象，还不能理解或者直接按照一个对象列表来用，这个就是符合查询条件（这个条件仅仅是查询结果之前的条件）的数据
		long totalHits = hits.getTotalHits();
		System.out.println("test15 response totalHits:"+totalHits);
		
		SearchHit[] hits2 = hits.getHits();//The hits of the search request (based on the search type, and from / size provided).  这个就是符合查询条件，并且符合查询结果的筛选条件的数据，所以就会出现符合查询条件的是1条，但是筛选完之后肯那个是0条
		for(SearchHit searchHit : hits2){
			//获取高亮内容
			Map<String,HighlightField> highlightFields = searchHit.getHighlightFields();
			//根据高亮字段获取内容
			HighlightField highlightField = highlightFields.get("city");
			Text[] fragments = highlightField.getFragments();
			for(Text text:fragments){
				System.out.println("test15 response text:"+text);
			}
			System.out.println("test15 response searchHit:"+searchHit.getSourceAsString());
		}

	}
	
	/**
	 * @删除 索引库
	 * 
	 */
	@Test
	public void test16() throws Exception{//这里是删除指定索引库，慎用！就不测试了，我还需要这些数据
		DeleteIndexResponse response = transportClient.admin().indices().prepareDelete(index).get();
	}
}
