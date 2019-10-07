package com.bona.mapreduce.flowsort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowSortDriver {
	//本job基于flowcount的输出结果（手机号 上行总流量 下行总流量 总流量），进行排序
	public static void main(String[] args) throws Exception {
		// 1 获取job信息
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		// 2 获取jar的存储路径
		job.setJarByClass(FlowSortDriver.class);

		// 3 关联map和reduce的class类
		job.setMapperClass(FlowSortMapper.class);
		job.setReducerClass(FlowSortReducer.class);

		// 4 设置map阶段输出的key和value类型
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);

		// 5 设置最后输出数据的key和value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

//		// 8设置分区
		job.setPartitionerClass(FlowPartitioner.class);
		// 设置reducenum
		job.setNumReduceTasks(5);
		
		// 6 设置输入数据的路径 和输出数据的路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 7 提交
		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);
	}
}
