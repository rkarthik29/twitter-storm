package org.karthik.hdp.twitter_storm;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterSpout implements IRichSpout{

    SpoutOutputCollector collector;
    int i=0;
    private LinkedBlockingQueue<Status> queue;
	public void ack(Object arg0) {
		// TODO Auto-generated method stub
		
	}
	public void activate() {
		// TODO Auto-generated method stub
		
	}
	public void close() {
		// TODO Auto-generated method stub
		
	}
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}
	public void fail(Object arg0) {
		// TODO Auto-generated method stub
		
	}
	public void nextTuple() {
		// TODO Auto-generated method stub
		Status ret = queue.poll();
		if (ret == null) {
			Utils.sleep(50);
        } else {
			collector.emit(new Values(ret));
		}
		
	}
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		this.collector=arg2;
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("MuoxafDLgdeEbrzo0peOW0iq3")
		  .setOAuthConsumerSecret("bdLLVYuI2N2NvG8Et2i2r1QqbjO1ePQSZs8LRF8ePk0a59fdac")
		  .setOAuthAccessToken("75307823-G9cNC8yyJkmrUs9RJkqFWv7ySf36vkz0SN1yrE9YH")
		  .setOAuthAccessTokenSecret("UyVyRcjMve7kLfwQn2yXsGu9LV8Yf3aTBI188JD9nV8Xg");
		TwitterStreamFactory tf = new TwitterStreamFactory(cb.build());
		TwitterStream twitter = tf.getInstance();
		queue =  new LinkedBlockingQueue<Status>(1000);
		twitter.addListener(new TwitterStatusListener(queue));
		FilterQuery fq= new FilterQuery();
		//fq.track("cloudera","hortonworks","mapr","bigdata","hdfs","hadoop");
		fq.track("#NFL","#NFL2016","#football","#NFC","#AFC","@NFL","@NFL2016");
		twitter.filter(fq);
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("tweet"));
		
	}
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
    
    

}
