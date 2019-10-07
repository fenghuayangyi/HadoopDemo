package com.bona.mapreduce.flowcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowDriver {

	public static void main(String[] args) throws Exception {
		// 1 ��ȡjob��Ϣ
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		// 2 ��ȡjar�Ĵ洢·��
		job.setJarByClass(FlowDriver.class);

		// 3 ����map��reduce��class��
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);

		// 4 ����map�׶������key��value����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		// 5 �������������ݵ�key��value����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

//		// 8���÷���
//		job.setPartitionerClass(FlowPartitioner.class);
//		// ����reducenum
//		job.setNumReduceTasks(5);
		
		// 6 �����������ݵ�·�� ��������ݵ�·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 7 �ύ
		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);
	}
}
