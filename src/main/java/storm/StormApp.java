package storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import storm.bolt.UpperCaseBolt;
import storm.bolt.WriteToFileBolt;
import storm.spout.TwitterSpout;

public class StormApp {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();        
		builder.setSpout("spout", new TwitterSpout()); 
		builder.setBolt("toUpperCaseBolt", new UpperCaseBolt(), 2).shuffleGrouping("spout");
		builder.setBolt("toFileBolt", new WriteToFileBolt(), 2).shuffleGrouping("toUpperCaseBolt");
	
		Config conf = new Config();
		conf.setDebug(false);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
	}

}
