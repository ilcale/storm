package storm.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

public class TwitterSpout extends BaseRichSpout {

	private static final long serialVersionUID = -1114066973180960191L;
	private static final Logger LOG = Logger.getLogger(TwitterSpout.class);
	
	SpoutOutputCollector collector;
	LinkedBlockingQueue<Status> queue = null;
	TwitterStream twitterStream;

	public void nextTuple() {
		final Status status = queue.poll();
		if (status == null) {
			Utils.sleep(50);
		} else {
			collector.emit(new Values(status), status.getId());
		}
	}

	public void open(@SuppressWarnings("rawtypes") Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
		this.collector = collector;
		this.queue = new LinkedBlockingQueue<Status>();
		
		twitterStream = new TwitterStreamFactory().getInstance();
	    // Listener
		StatusListener listener = new StatusListener() {
			
			public void onException(Exception arg0) {
				// TODO Auto-generated method stub
				
			}
			
			public void onTrackLimitationNotice(int arg0) {
				// TODO Auto-generated method stub
				
			}
			
			public void onStatus(Status status) {
				queue.offer(status);
			}
			
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub
				
			}
			
			public void onScrubGeo(long arg0, long arg1) {
				// TODO Auto-generated method stub
				
			}
			
			public void onDeletionNotice(StatusDeletionNotice arg0) {
				// TODO Auto-generated method stub
				
			}
		};
	    twitterStream.addListener(listener);

	    // Filter
	    FilterQuery filtre = new FilterQuery();
	    String[] keywordsArray = { "walmart", "wal-mart" };
	    filtre.track(keywordsArray);
	    twitterStream.filter(filtre);

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}
	
	@Override
	public void fail(Object msgId) {
		Twitter twitter = new TwitterFactory().getInstance();
        try {
			Status status = twitter.showStatus((Long) msgId);
			queue.offer(status);
			LOG.info("TWEET FAILED: " + status);
		} catch (TwitterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
