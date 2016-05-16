package pagerank;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PageRankPartitioner extends Partitioner<Text, Text> {
	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {

		String keyFirstLetter = key.toString().substring(0, 1);
		if(keyFirstLetter.matches("[a-gA-G]")) {
			return 0;
		}
		else {
			return 1;
		}
	}
}
