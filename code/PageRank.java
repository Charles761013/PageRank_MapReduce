package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {

	//Count: Number of Titles, PageCountTotal: Total PageRank of all pages
	static enum PageCount {
		Count,
		PageRankTotal
	}

	public static void main(String[] args) throws Exception {

		boolean status = false;
		long error = 0;
		String tempOutput1 = "PageRank/temp1";
		String tempOutput2 = "PageRank/temp2";
		long t0, t1;
		t0 = System.currentTimeMillis();
		System.out.println("t0 Time: " + String.valueOf(t0));

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "BuildGraph");
		job.setJarByClass(PageRank.class);
		// set the class of each stage in mapreduce
		job.setMapperClass(BuildGraphMapper.class);
		//job.setPartitionerClass(PageRankPartitioner.class);
		job.setReducerClass(BuildGraphReducer.class);
		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// set the number of reducer
		job.setNumReduceTasks(1);
		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(tempOutput1));
		status = job.waitForCompletion(true);
		long pageNumber = job.getCounters().findCounter(PageCount.Count).getValue();
		////////////////////////////////////////////////
		//Start Iteration
		int iter = 0;
		final int iterLimit = 20000;
		String fileInput = tempOutput1;
		String fileOutput = tempOutput2;
		while(iter < iterLimit)
		{
			if((iter % 2) == 0) {
				fileInput = tempOutput1;
				fileOutput = tempOutput2;
			}
			else {
				fileInput = tempOutput2;
				fileOutput = tempOutput1;
			}
			
			System.out.println("Number of Iteration: " + iter);
			Configuration confPR = new Configuration();
			FileSystem fs = FileSystem.get(confPR);
			fs.delete(new Path(fileOutput), true);

			confPR.setLong("PageNumber", pageNumber);
			Job jobPR = Job.getInstance(confPR, "PageRank");
			jobPR.setJarByClass(PageRank.class);
			jobPR.setMapperClass(PageRankMapper.class);
			jobPR.setReducerClass(PageRankReducer.class);

			jobPR.setMapOutputKeyClass(Text.class);
			jobPR.setMapOutputValueClass(Text.class);
			jobPR.setOutputKeyClass(Text.class);
			jobPR.setOutputValueClass(Text.class);
			jobPR.setNumReduceTasks(1);
			FileInputFormat.addInputPath(jobPR, new Path(fileInput));
			FileOutputFormat.setOutputPath(jobPR, new Path(fileOutput));
			status = jobPR.waitForCompletion(true);
			error = jobPR.getCounters().findCounter(PageCount.PageRankTotal).getValue();
			System.out.println("error:" + String.valueOf(error));
			if(error < 10) {
				
				break;
			}
			iter++;
		}
		//sort result
		Configuration confST = new Configuration();
		Job jobST = Job.getInstance(confST, "PRSorting");
		jobST.setJarByClass(PageRank.class);
		jobST.setMapperClass(PRSortingMapper.class);
		jobST.setSortComparatorClass(PRSortingKeyComparator.class);
		jobST.setReducerClass(PRSortingReducer.class);

		jobST.setMapOutputKeyClass(Text.class);
		jobST.setMapOutputValueClass(Text.class);
		jobST.setOutputKeyClass(Text.class);
		jobST.setOutputValueClass(Text.class);
		jobST.setNumReduceTasks(1);
		FileInputFormat.addInputPath(jobST, new Path(fileOutput));
		FileOutputFormat.setOutputPath(jobST, new Path(args[1]));
		status = jobST.waitForCompletion(true);
		//////////////////////////////
		t1 = System.currentTimeMillis();
		System.out.println("t1 Time: " + String.valueOf(t1));
		System.out.println("Run Time: " + String.valueOf(t1-t0) + "ms.");
		System.exit(status ? 0 : 1);

	}
}
