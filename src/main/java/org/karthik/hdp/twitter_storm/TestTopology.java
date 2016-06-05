package org.karthik.hdp.twitter_storm;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.RecordWriter;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.hive.hcatalog.streaming.TransactionBatch;
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
import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.bolt.mapper.HiveMapper;
import org.apache.storm.hive.bolt.mapper.JsonRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import storm.trident.tuple.TridentTuple;
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

        String metaStoreURI = "thrift://node3.xyzcorp.hadoop.com:9083";
        String dbName = "default";
        String tblName = "nfl_tweets";
        String[] partNames = {"team"};
        String[] colNames = {"createdat", "tweet", "sentiment"};
        
        HiveMapper mapper = new HiveMapper() {
			
			public void write(TransactionBatch arg0, Tuple arg1) throws StreamingException, IOException, InterruptedException {
				// TODO Auto-generated method stub
				
			}
			
			public byte[] mapRecord(TridentTuple arg0) {
				// TODO Auto-generated method stub
				return null;
			}
			
			public byte[] mapRecord(Tuple arg0) {
				// TODO Auto-generated method stub
				return null;
			}
			
			public List<String> mapPartitions(TridentTuple arg0) {
				// TODO Auto-generated method stub
				return null;
			}
			
			public List<String> mapPartitions(Tuple arg0) {
				// TODO Auto-generated method stub
				return null;
			}
			
			public RecordWriter createRecordWriter(HiveEndPoint arg0)
					throws StreamingException, IOException, ClassNotFoundException {
				// TODO Auto-generated method stub
				return null;
			}
		};
        DelimitedRecordHiveMapper tweet_mapper = new DelimitedRecordHiveMapper()
        	       .withColumnFields(new Fields(colNames))
        	       .withPartitionFields(new Fields(partNames));
        HiveOptions hiveOptions = new HiveOptions(metaStoreURI, dbName, tblName, tweet_mapper);
        
        hiveOptions.withTxnsPerBatch(20).withBatchSize(100).withIdleTimeout(60);
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
        //topology.setBolt("hdfsbolt",bolt).shuffleGrouping("sampleSpout");
        topology.setBolt("sampleBolt",new ConsoleBolt()).shuffleGrouping("sampleSpout");
        //topology.setBolt("afcBolt", afcBolt).shuffleGrouping("sampleBolt", "AFC");
        //topology.setBolt("nfcBolt", nfcBolt).shuffleGrouping("sampleBolt", "NFC");
        topology.setBolt("tweetstohive", new HiveBolt(hiveOptions)).shuffleGrouping("sampleBolt", "tweets");
        if(args.length==0){
	        Config conf1 = new Config();
	        conf1.setDebug(false);
	        conf1.put("HBCONFIG", HBConfig);
	        LocalCluster cluster=new LocalCluster();
	        cluster.submitTopology("test2", conf1, topology.createTopology());
        }else{
        	//Config conf = new Config();
        	try{
        	StormSubmitter.submitTopology("mytwittertopology",conf, topology.createTopology());
        	}catch(Exception e){
        		System.out.println(e.getMessage());
        	}
        }


    }

}
