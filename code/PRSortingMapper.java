package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

public class PRSortingMapper extends Mapper<LongWritable, Text, Text, Text> {

	//private Text keyResult = new Text();
	//private Text valResult = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		StringBuffer keyCombine = new StringBuffer();
		String[] pair = value.toString().split("\t"); //key-value default separator
		String keyNew = pair[0];
		String valueNew = pair[1];
		String[] prLink = valueNew.split("#", 2); //separate Pagerank and Link
		String pageRankVal = prLink[0];
		keyCombine.append(pageRankVal);
		keyCombine.append("#");
		keyCombine.append(keyNew);
		context.write(new Text(keyCombine.toString()), new Text(pageRankVal));		
	}

}


