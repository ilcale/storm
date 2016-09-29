package storm.bolt;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.Status;

public class UpperCaseBolt extends BaseRichBolt {

	private static final long serialVersionUID = -2340021955729282606L;
	private static final Logger LOG = Logger.getLogger(UpperCaseBolt.class);
	
	private OutputCollector collector;

	public void execute(Tuple input) {
		Status tweet = (Status) input.getValueByField("tweet");
		LOG.info("UpperCaseBolt: " + tweet.getId());
			if(tweet.getLang().contains("en")){
				LOG.error("UpperCaseBolt Fail: " + tweet.getUser().getScreenName() + " - " + tweet.getId());
				collector.fail(input);
			}else{
				collector.emit(new Values(tweet.getUser().getScreenName() + " - " + tweet.getText().toUpperCase()));
				collector.ack(input);
			}
	}

	public void prepare(@SuppressWarnings("rawtypes") Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}
