package com.qingmin.test.elasticsearch;

import java.util.HashMap;
import java.util.List;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class ESTestScore {

		
		TransportClient transportClient = new TransportClient();
		String index = "score";
		//String type = "score-1";
		String type = "score-2";
		
		@Before
		public void connection() throws Exception{
			transportClient.addTransportAddress(new InetSocketTransportAddress("192.168.56.130",9300));
			
		}
		
		/**
		 * @删除 索引库
		 * 
		 */
		@Ignore
		public void test1() throws Exception{//这里是删除指定索引库，慎用！就不测试了，我还需要这些数据
			//DeleteIndexResponse response = transportClient.admin().indices().prepareDelete("testsettings").get();
			DeleteResponse response = transportClient.prepareDelete(index, type, "w").get();//删除一条数据
		}
		
		/**
		 * @初始化数据就是 添加数据
		 */
		@Test
		public void test2() throws Exception{
			
			XContentBuilder builder = XContentFactory.jsonBuilder()
					.startObject()
					.field("name","haha")
					.field("score",60)
					.endObject();
			IndexResponse response = transportClient.prepareIndex(index, type).setSource(builder).get();
			System.out.println(response.getVersion());
			
		}
		
		/**
		 * @根据字段进行分组，统计分组字段的和，在这里也就是计算每个年龄总共多少人 相当于select count(*),age from table group by age
		 */
		@Ignore
		public void test3() throws Exception{
			
			
			SearchResponse response = transportClient.prepareSearch(index)
					                 .setTypes(type).addAggregation(AggregationBuilders.terms("age_term").field("age"))//这里面的term就是分组功能，对age进行分组
					                 .get();
			Terms terms = response.getAggregations().get("age_term");
			List<Bucket> bukets = terms.getBuckets();
			for(Bucket bucket: bukets){
				System.out.println(bucket.getKey()+":"+bucket.getDocCount());
			}
			
			
		}
		
		/**
		 * @根据字段分组，统计其它字段的和，在这里也就是计算统计每个人的总成绩 相当于 select sum(score) as sumScore,name from table group by name 
		 * 默认情况下只能返回前10组 ,返回所有组就是size(0)
		 */
		@Ignore
		public void test4() throws Exception{
			SearchResponse response = transportClient.prepareSearch(index).setTypes(type)
			                         .addAggregation(AggregationBuilders.terms("name_terms").field("name")
			                         .size(0)//必须在分组字段后面设置，才是把所有的分组都查出来，不然不起效果
			                		 .subAggregation(AggregationBuilders.sum("sumScore").field("score")))
					                 .get();
			Terms terms = response.getAggregations().get("name_terms");
			List<Bucket> bukets = terms.getBuckets();
			for(Bucket bucket: bukets){
				Sum sum = bucket.getAggregations().get("sumScore");
				
				System.out.println(bucket.getKey()+":"+sum.getValue());
			}
		}
		
		/**
		 * @分页 sql的分页是用limit来实现的，在es里面是用from和size两个参数来做的
		 * 
		 */
		@Ignore
		public void test5() throws Exception{
			SearchResponse response = transportClient.prepareSearch(index).setTypes(type)
			                         .addAggregation(AggregationBuilders.terms("name_terms").field("name")
			                         .size(0)//必须在分组字段后面设置，才是把所有的分组都查出来，不然不起效果
			                		 .subAggregation(AggregationBuilders.sum("sumScore").field("score")))
					                 .get();
			Terms terms = response.getAggregations().get("name_terms");
			List<Bucket> bukets = terms.getBuckets();
			for(Bucket bucket: bukets){
				Sum sum = bucket.getAggregations().get("sumScore");
				
				System.out.println(bucket.getKey()+":"+sum.getValue());
			}
		}
		
		/**
		 * @对于不存在的索引库对settings属性进行修改
		 * 
		 */
		@Ignore
		public void test6() throws Exception{
			HashMap<String,Object> settings = new HashMap<String,Object>();
			settings.put("number_of_shards", 1);
			settings.put("number_of_replicas", 1);
			CreateIndexRequestBuilder createIndexResponseBuilder = transportClient.admin().indices().prepareCreate("testsettings");
			createIndexResponseBuilder.setSettings(settings).get();
		}
		
		/**
		 * @对于已经存在的索引库对settings属性进行修改
		 * 
		 */
		@Ignore
		public void test7() throws Exception{
			HashMap<String,Object> settings = new HashMap<String,Object>();
			settings.put("number_of_replicas", 2);
			UpdateSettingsRequestBuilder updateSettingsRequestBuilder = transportClient.admin().indices().prepareUpdateSettings("testsettings");
			updateSettingsRequestBuilder.setSettings(settings).get();
		}
}
