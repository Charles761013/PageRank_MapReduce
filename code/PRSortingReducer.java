package pagerank;

import java.io.*;
import java.util.Iterator;
import java.lang.Integer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

public class PRSortingReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String[] keySep = key.toString().split("#");
		String pageRankVal = keySep[0];
		String title = keySep[1];
		context.write(new Text(title), new Text(pageRankVal));		
	}
}