package MR.HW3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import MR.HW3.PreProcessingMapper;
import MR.HW3.PreProcessingReducer;

public class PageRankDriver {
	//apha term 
	static final double DAMPING_FACTOR = 0.15;
	//nodecounter - for keeping the count of nodes
	//deltaCounter- for calulating the contribution of dangling nodes
	public static enum COUNTERS{
		nodeCount,
		deltaCounter;
	}
	public static void main(String[] args) throws Exception {
		//driver program
		double previousDelta = 0.00;
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: pagerank <in> [<in>...] <out>");
			System.exit(2);
		}
		//parser job execution
		Job job = Job.getInstance(conf, "page rank");
		job.setJarByClass(PageRankDriver.class);
		job.setMapperClass(PreProcessingMapper.class);
		job.setReducerClass(PreProcessingReducer.class);
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		job.waitForCompletion(true);
		//update with initial pagerank values

		conf.set("nodeCount",String.valueOf(job.getCounters().findCounter(COUNTERS.nodeCount).getValue()));

		
		//update initial page rank values to be 1/N(Map only job)
		
		Job jobInit = Job.getInstance(conf, "update initial page rank");
		jobInit.setJarByClass(PageRankDriver.class);
		jobInit.setMapperClass(PageRankInitialiseMapper.class);
		jobInit.setOutputKeyClass(Text.class); 
		jobInit.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(jobInit, new Path(otherArgs[otherArgs.length - 1]));
		FileOutputFormat.setOutputPath(jobInit, new Path(otherArgs[otherArgs.length - 1]+"1"));
		jobInit.waitForCompletion(true);
		Path inputPath = new Path(otherArgs[otherArgs.length - 1]+"1");
		
		//run 10 iteration of page rank
		int i;
		for(i=0;i<10;i++) {
			conf.set("previousDanglingContribution",String.valueOf(previousDelta));
			//Path outputPath = new Path("out/pagerank"+i);
			Path outputPath = new Path(otherArgs[otherArgs.length - 1]+"PageRank"+(i+1));
			Job job1 = Job.getInstance(conf, "pagerank calc");
			job1.setJarByClass(PageRankDriver.class);
			job1.setMapperClass(PageRankMapper.class);
			job1.setReducerClass(PageRankReducer.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job1,inputPath );
			FileOutputFormat.setOutputPath(job1,outputPath );
			job1.waitForCompletion(true);
			inputPath = outputPath;
			previousDelta = (double) job1.getCounters().findCounter(COUNTERS.deltaCounter).getValue();
			job1.getCounters().findCounter(COUNTERS.deltaCounter).setValue(0);
			job1.waitForCompletion(true);
		}
		
		
		//get the top 100 results using tok-records algo
		Job job2 = new Job(conf, "Top 100 pages PageRank");
		job2.setJarByClass(PageRankDriver.class);
		job2.setMapperClass(TopKMapper.class);
		job2.setReducerClass(TopKReducer.class);
		job2.setNumReduceTasks(1);
		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(otherArgs[otherArgs.length - 1]+"PageRank"+i));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1]+"Top100"));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		
	}
}
