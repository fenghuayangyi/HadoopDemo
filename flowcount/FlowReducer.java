package com.bona.mapreduce.flowcount;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context)
			throws IOException, InterruptedException {
		// 1 计算总的流量
		long sum_upFlow = 0;
		long sum_downFlow = 0;

		for (FlowBean bean : values) {
			sum_upFlow += bean.getUpFlow();
			sum_downFlow += bean.getDownFlow();
		}

		// 2 输出
		context.write(key, new FlowBean(sum_upFlow, sum_downFlow));

	}
}
