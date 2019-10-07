package com.bona.mapreduce.order;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
	OrderBean k = new OrderBean();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// 1 ��ȡһ��
		String line = value.toString();
		
		// 2 ��ȡ
		String[] fields = line.split("\t");
		
		// 3 ��װ����
		k.setOrder_id(Integer.parseInt(fields[0]));
		k.setPrice(Double.parseDouble(fields[2]));
		
		// 4 д��
		context.write(k, NullWritable.get());
	}
}

