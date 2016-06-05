package org.karthik.hdp.twitter_storm;

import java.util.HashMap;
import java.util.Map;

public class TweetAggregator {
	private long key=0;
	private long prev_key=0;
	private Map<String,Integer> applications;
	private Map<String,Integer> prev_applications;
	
	public TweetAggregator(){
		this.applications = new HashMap<String, Integer>();
	}
	public boolean setKey(long key){
		//System.out.println(this.key+"--"+key+"--"+this.prev_key);
		if(this.key!=key ){
			if(this.prev_key==0){
				this.prev_key=key;
				this.key=key;
				prev_applications=applications;
				return false;
			}else{
				this.prev_key=this.key;
				this.key=key;
				prev_applications=applications;
				return true;
			}
			
		}
		return false;
	}
	
	public void incCount(String key){
		//System.out.println(key);
		Integer count=applications.get(key);
		if(count==null){
			applications.put(key,1);
		}else{
			applications.put(key,count.intValue()+1);
		}
		
	}
	
	public Map<String,Integer> getPrevApplications(){
		return prev_applications;
	}
	
	public long getPrev_key(){
		return this.prev_key;
	}

}
