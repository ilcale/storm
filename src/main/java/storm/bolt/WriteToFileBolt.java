package storm.bolt;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class WriteToFileBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1288306000201536185L;
	private static final Logger LOG = Logger.getLogger(WriteToFileBolt.class);

	private OutputCollector collector;

	public void execute(Tuple input) {
		LOG.info("WriteToFileBolt: " + input);
		String tweet = input.getStringByField("tweet");
		File file = new File("out.txt");
		try {
			if (!file.exists()) {
				file.createNewFile();
			}
			FileWriter fileWriter = new FileWriter(file, true);
			fileWriter.write(tweet + "\n ---------------**-------------- \n");
			fileWriter.close();
//			throw new IOException("some IOexception");
		} catch (IOException e) {
			e.printStackTrace();
			collector.fail(input);
		}
	}

	public void prepare(@SuppressWarnings("rawtypes") Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

}
