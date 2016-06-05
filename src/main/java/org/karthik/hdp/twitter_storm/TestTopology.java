package org.karthik.hdp.twitter_storm;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import twitter4j.Status;

public class TestTopology {

    /**
     * @param args
     */
    public static void main(String[] args) {

        TopologyBuilder topology=new TopologyBuilder();
        
        RecordFormat rf = new RecordFormat() {
			
			public byte[] format(Tuple tuple) {
				// TODO Auto-generated method stub
				Status status = (Status)tuple.getValueByField("tweet");
				return status.toString().getBytes();
			}
		};
     // sync the filesystem after every 1k tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // rotate files when they reach 5MB
       FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/user/storm/nflstats/")
                .withPrefix("tweets")
                .withExtension(".txt");
       FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(64.0f, Units.MB);

        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl("hdfs://node2.xyzcorp.hadoop.com:8020")
                .withRecordFormat(rf)
                .withFileNameFormat(fileNameFormat)
                .withSyncPolicy(syncPolicy)
                .withRotationPolicy(rotationPolicy);
        
        SimpleHBaseMapper afcmapper = new SimpleHBaseMapper() 
                .withRowKeyField("timestamp")
                .withColumnFields(new Fields("stats"))
                .withColumnFamily("afc");
        
        SimpleHBaseMapper nfcmapper = new SimpleHBaseMapper() 
                .withRowKeyField("timestamp")
                .withColumnFields(new Fields("stats"))
                .withColumnFamily("nfc");
        
        Map<String, String> HBConfig = new HashMap<String, String>();
        HBConfig.put("hbase.conf","/etc/hbase/conf");
        Map conf = new HashMap();
    	conf.put(Config.NIMBUS_HOST, "node2.xyzcorp.hadoop.com"); //YOUR NIMBUS'S IP
    	conf.put(Config.NIMBUS_THRIFT_PORT,6627); 
    	conf.put("HBCONFIG", HBConfig);
        HBaseBolt afcBolt = new HBaseBolt("nflstats", afcmapper).withConfigKey("HBCONFIG");
        HBaseBolt nfcBolt = new HBaseBolt("nflstats", nfcmapper).withConfigKey("HBCONFIG");
        topology.setSpout("sampleSpout",new TwitterSpout());
        topology.setBolt("hdfsbolt",bolt).shuffleGrouping("sampleSpout");
        topology.setBolt("sampleBolt",new ConsoleBolt()).shuffleGrouping("sampleSpout");
        topology.setBolt("afcBolt", afcBolt).shuffleGrouping("sampleBolt", "AFC");
        topology.setBolt("nfcBolt", nfcBolt).shuffleGrouping("sampleBolt", "NFC");
        if(args.length==0){
	        Config conf1 = new Config();
	        conf1.setDebug(false);
	        conf1.put("HBCONFIG", HBConfig);
	        LocalCluster cluster=new LocalCluster();
	        cluster.submitTopology("test", conf1, topology.createTopology());
        }else{
        	//Config conf = new Config();
        	try{
        	StormSubmitter.submitTopology("mytopology",conf, topology.createTopology());
        	}catch(Exception e){
        		System.out.println(e.getMessage());
        	}
        }


    }

}
