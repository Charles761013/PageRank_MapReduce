package pagerank;

import java.io.IOException;
import java.util.Iterator;
import java.lang.Integer;
import java.lang.StringBuffer;
import java.util.HashMap;
import pagerank.PageRank.PageCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapred.*;

public class BuildGraphReducer extends Reducer<Text, Text, Text, Text> {

	private Text valResult = new Text();
	private Text keyResult = new Text();
	private HashMap<String, Integer> titleMap = new HashMap<String, Integer>();

	private long mapperCounter = 0;

	@Override
	public void setup(Context context) throws IOException, InterruptedException{
		Configuration conf = context.getConfiguration();
		Cluster cluster = new Cluster(conf);
		Job currentJob = cluster.getJob(context.getJobID());
		mapperCounter = currentJob.getCounters().findCounter(PageCount.Count).getValue();
	}

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		StringBuffer linkBuffer = new StringBuffer();
		double initialPR = (1.0 / mapperCounter);
		boolean isMagicPair = false;
		if(key.toString().compareTo("    ") == 0) {
			isMagicPair = true;
		}

		linkBuffer.append(String.valueOf(initialPR));
		Iterator iter = values.iterator();
		while(iter.hasNext()) {
			String temp = iter.next().toString();
			if(isMagicPair) {
				titleMap.put(temp, 1);
				continue;
			}
			else {
				//check for valid out link
				//StringBuffer linkBuf = new StringBuffer();
				//linkBuf.append(temp.substring(0, 1).toLowerCase());
				/*if(temp.length() > 1) {
					linkBuf.append(temp.substring(1));
				}*/
				if((titleMap.get(temp) == null)) {
					continue; //don't append string
				}
			}

			linkBuffer.append("#");
			linkBuffer.append(temp);
		}

		if(isMagicPair) {
			return;
		}

		valResult.set(linkBuffer.toString());
		// write the result
		context.write(key, valResult);

	}
}
