package com.confiz;

import com.confiz.bolts.WordCounter;
import com.confiz.bolts.WordNormalizer;
import com.confiz.spouts.SignalsSpout;
import com.confiz.spouts.WordReader;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.ClusterSummary;
import backtype.storm.topology.TopologyBuilder;

public class TopologyMain {

	public static void main(String[] args) throws InterruptedException {
		try {
			TopologyBuilder builder = new TopologyBuilder();
			builder.setSpout("word-reader", new WordReader());
			builder.setSpout("signals-spout", new SignalsSpout());
			
			builder.setBolt("word-normalizer", new WordNormalizer())
					.shuffleGrouping("word-reader");
			
			builder.setBolt("word-counter", new WordCounter(), 2)
					.shuffleGrouping("word-normalizer")
					.allGrouping("signals-spout", "signals");

			Config conf = new Config();
			conf.put("wordsFile", "E:\\work\\maven\\test-repo\\my-own-storm\\src\\main\\java\\resources\\words.txt");
			conf.setDebug(true);
			conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
			
			Thread.sleep(4000);
			
//			ClusterSummary clusterInfo = cluster.getClusterInfo();
//			cluster.shutdown();
			
		} catch(Exception ioe) {
			System.out.println("################ Exception thrown ################");
			ioe.printStackTrace();
		}
	}

}
