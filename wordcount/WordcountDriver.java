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
		//ָ����ҵ��jobҪʹ�õ�mapper/Reducerҵ����/�������Լ�������Ӧ������
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);
		job.setPartitionerClass(WordCountPartitioner.class);
		job.setNumReduceTasks(2);
		//ָ��mapper������ݵ�kv����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//ָ��������������ݵ�kv����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// job.submit();  waitForCompletion����submit
		boolean result = job.waitForCompletion(true);
		System.exit(result?0:1);
	}
}
