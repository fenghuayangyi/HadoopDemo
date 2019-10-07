package com.bona.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordcountDriver {
	public static void main(String[] args) throws Exception{
		Configuration configuration = new Configuration();
//		configuration.set("mapreduce.framework.name", "yarn");
//		configuration.set("yarn.resourcemanager.hostname", "bonacluster1");
		Job job = Job.getInstance(configuration);
		job.setJarByClass(WordcountDriver.class);
		//指定本业务job要使用的mapper/Reducer业务类/分区类以及分区对应的数量
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);
		job.setPartitionerClass(WordCountPartitioner.class);
		job.setNumReduceTasks(2);
		//指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//指定最终输出的数据的kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// job.submit();  waitForCompletion包含submit
		boolean result = job.waitForCompletion(true);
		System.exit(result?0:1);
	}
}
