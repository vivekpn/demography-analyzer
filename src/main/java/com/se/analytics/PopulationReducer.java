package com.se.analytics;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PopulationReducer implements
		Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	public void configure(JobConf arg0) {

	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public void reduce(Text term, Iterator<LongWritable> values,
			OutputCollector<Text, LongWritable> context, Reporter arg3)
			throws IOException {
		long sum = 0;
		while (values.hasNext()) {
			LongWritable longWritable = values.next();
			sum += longWritable.get();
		}
		context.collect(term, new LongWritable(sum));
		System.out.print(term);
		System.out.print(",");
		System.out.println(sum);
	}
}
