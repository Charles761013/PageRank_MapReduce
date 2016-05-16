package pagerank;

import java.io.IOException;
import java.util.Iterator;
import java.lang.Integer;
import java.lang.StringBuilder;
import pagerank.PageRank.PageCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

	private Text valResult = new Text();
	final double dpFactor = 0.85;
	private long totalPage = 0;
	private double totalDanglingVal = 0.0;
	private double err = 0.0;

	/*@Override
	public void setup(Context context) throws IOException, InterruptedException{
		Configuration conf = context.getConfiguration();
		Cluster cluster = new Cluster(conf);
		Job currentJob = cluster.getJob(context.getJobID());
		totalPage = currentJob.getCounters().findCounter(PageCount.Count).getValue();
	}*/

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		totalPage = conf.getLong("PageNumber", 100);
		StringBuffer outLink = new StringBuffer();
		StringBuffer output = new StringBuffer();
		double pagerankOld = 0.0;
		double prTotal = 0.0;

		if(key.toString().compareTo("    ") == 0) {
			for(Text val: values) {
				totalDanglingVal += Double.parseDouble(val.toString());
			}
			return;
		}
		//System.out.printf("@@@@totalDanglingSum = %10f\n", totalDanglingVal);
		//System.out.printf("@@@@totalPage = %d\n", totalPage);
		//System.out.println("----Title---: " + key.toString());

		for(Text v: values) {
			String bufferVal = v.toString();
			String[] val = bufferVal.split("#", 2);
			//int ret = val.indexOf(".");
			//System.out.println("val :" + val);
			//boolean ret = val.matches("-?\\d+(\\.\\d+)?(E-?\\d+)?");
			if(val[0].compareTo("c") == 0) {
				if(Double.parseDouble(val[1]) > 0.00001)
					System.out.printf("val = %10f\n", Double.parseDouble(val[1]));
				prTotal += Double.parseDouble(val[1]);
			}
			else {
				if(val[0].compareTo("a") == 0) {
					//this is old pagerank value
					pagerankOld = Double.parseDouble(val[1]);
					//System.out.printf("@@@@@ pagerankOld = %f\n", pagerankOld);
				}
				else if (val[0].compareTo("b") == 0){
					outLink.append("#");
					outLink.append(val[1]);
				}
			}
		}
		 //(double)(dpFactor*prTotal) + (dpFactor*((double)(totalDanglingVal/totalPage)));
		prTotal = ((double)(1-dpFactor)/((double)totalPage)) + (double)dpFactor*(prTotal + (totalDanglingVal/(double)totalPage));
		//System.out.printf("@@@@@prTotal = %10f\n", prTotal);
		err = err + (double)Math.abs(prTotal - pagerankOld);
		//System.out.printf("@@@@@err = %10f\n", err);

		output.append(prTotal);
		output.append(outLink);
		valResult.set(output.toString());
		context.write(key, valResult);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.getCounter(PageCount.PageRankTotal).increment((long) (err*10000));
	}
}
