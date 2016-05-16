package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

	//private Text keyResult = new Text();
	//private Text valResult = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] pair = value.toString().split("\t"); //key-value default separator
		String keyNew = pair[0];
		String valueNew = pair[1];
		String[] prLink = valueNew.split("#", 2); //separate Pagerank and Link
		String pageRankVal = prLink[0];
		//System.out.printf("@@@@@@pageRankVal = %s\n", pageRankVal);
		StringBuffer buffer = new StringBuffer();
		buffer.append("a#");
		buffer.append(pageRankVal);
		context.write(new Text(keyNew), new Text(buffer.toString())); //record the old pagerank value

		if(prLink.length > 1) {
			String outLink = prLink[1];
			buffer.setLength(0);
			buffer.append("b#");
			buffer.append(outLink);
			//System.out.printf("@@@@@@outLink = %s\n", outLink);
			context.write(new Text(keyNew), new Text(buffer.toString())); //record the outLink
			//start to compute PageRank value a page credit
			String[] linkArray = outLink.split("#");
			double credit = Double.parseDouble(pageRankVal) / ((double)((linkArray.length)*1.0));
			for(int i = 0; i < linkArray.length; i++) {
				//keyResult.set(linkArray[i]);
				//valResult.set(String.valueOf(credit));
				buffer.setLength(0);
				buffer.append("c#");
				buffer.append(String.valueOf(credit));
				context.write(new Text(linkArray[i]), new Text(buffer.toString()));
			}
		}
		else {
			//String dummyLink = "0.00E1";
			context.write(new Text("    "), new Text(pageRankVal)); //give dangling node credit
			//context.write(new Text(keyNew), new Text(dummyLink)); //record the outLink
		}
	}

}


