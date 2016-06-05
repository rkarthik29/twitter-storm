package org.karthik.hdp.twitter_storm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonObject;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.HashtagEntity;
import twitter4j.Status;

public class ConsoleBolt implements IBasicBolt{
	
	private static final List<String> AFCTeams= new ArrayList<String>(Arrays.asList("bills","dolphins","patriots","jets",
											"ravens","bengals","browns"
											,"steelers","texans","colts","jaguars","titans",
											"broncos","chiefs","raiders","chargers"));
			
	private static final List<String> NFCTeams=new ArrayList<String>(Arrays.asList("cowboys","giants","eagles","redskins","bears",
											"lions","packers","vikings","falcons",
											"panther","saints","buccaneers","cardinals",
											"rams","saints","seahawks"));

	private static final String nfc="NFC";
	private static final String afc="AFC";

	private TweetAggregator agg;
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declareStream("AFC", new Fields("timestamp", "stats"));
		declarer.declareStream("NFC", new Fields("timestamp", "stats"));
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		Status status = (Status)input.getValueByField("tweet");
		Calendar cal = Calendar.getInstance();
		//cal.setTime(status.getCreatedAt());
		int minutes= cal.get(Calendar.MINUTE);
		minutes = (minutes/5);
		minutes = minutes*5;
		cal.set(Calendar.MINUTE, minutes);
		cal.set(Calendar.SECOND,0);
		cal.set(Calendar.MILLISECOND,0);
		long timestamp=cal.getTimeInMillis();
		String tweet=status.getText().toLowerCase();
		//if(status.getPlace()!=null && status.getPlace().getCountryCode().equals("US"))
		String team=null;
		String conference=null;
		for( HashtagEntity hash:status.getHashtagEntities()){
			String hashText= hash.getText();
			if(NFCTeams.contains(hashText.toLowerCase())){
				team=hashText.toUpperCase().charAt(0)+hashText.toLowerCase().substring(1);
				conference=nfc;
				break;
			}else if(AFCTeams.contains(hashText.toLowerCase())){
				team=hashText.toUpperCase().charAt(0)+hashText.toLowerCase().substring(1);
				conference=afc;
				break;
			}
				
		}
		if(team!=null){
			String location=null;
			if(status.getPlace()!=null && status.getPlace().getCountryCode().equals("US"))
				location=status.getPlace().getFullName();
			//collector.emit(new Values(timestamp,team.toString(),conference));
			boolean keyChanged=agg.setKey(timestamp);
			//System.out.println(timestamp+"--"+keyChanged);
			agg.incCount(conference+":"+team);
			if(keyChanged){
				JsonObject afcStats = new JsonObject();
				JsonObject nfcStats = new JsonObject();
				for(Map.Entry<String, Integer> entry:agg.getPrevApplications().entrySet()){
					String[] keys = entry.getKey().split(":");
					if(nfc.equals(keys[0]))
						nfcStats.addProperty(keys[1], entry.getValue());
					if(afc.equals(keys[0]))
						afcStats.addProperty(keys[1], entry.getValue());		
					//
					//System.out.println(keys[0]+"--"+agg.getPrev_key()+"--"+keys[1]+"--"+entry.getValue());
				}
				//System.out.println(nfc+"--"+agg.getPrev_key()+"--"+nfcStats);
				//System.out.println(afc+"--"+agg.getPrev_key()+"--"+afcStats);
				collector.emit(nfc,new Values(agg.getPrev_key(),nfcStats.toString()));
				collector.emit(afc,new Values(agg.getPrev_key(),afcStats.toString()));
			}
		}
	}

	public void prepare(Map arg0, TopologyContext arg1) {
		// TODO Auto-generated method stub
		this.agg = new TweetAggregator();
		
	}

}
