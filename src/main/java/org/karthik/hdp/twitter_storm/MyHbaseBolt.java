package org.karthik.hdp.twitter_storm;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class MyHbaseBolt implements IBasicBolt {
	
	private HConnection connection;
	private Exception hbaseException;
	private HTableInterface table;

	public void cleanup() {
		// TODO Auto-generated method stub
		try {
			 if(connection!=null){
				 connection.close();
			 }
		     System.out.println("hbase connection closed");
		  } catch (Exception e) {
		     System.out.println("cleanup error");
		  }

	}

	public void execute(Tuple arg0,BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		if(connection!=null){
		 try {
		      // use the connection
			 	/*Put p = new Put(key.getBytes());
			 	p.add();
			 	p.add();
			 	table.put(p);*/
		   } catch (Exception e) {
		      collector.reportError(e);
		   }
		}else{
			collector.reportError(this.hbaseException);
		}

	}
	

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@SuppressWarnings("rawtypes")
	public void prepare(Map arg0, TopologyContext arg1) {
		// TODO Auto-generated method stub
		Configuration config = HBaseConfiguration.create();
		try {
			connection = HConnectionManager.createConnection(config);
			table = connection.getTable("osanalytics");
			table.setAutoFlush(false, false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			hbaseException=e;
			e.printStackTrace();
		}
		
	}

}
