package com.se.analytics;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.se.data.DemographyEntry;
import com.se.file.CSVFileHandler;

public class PopulationPlotMR {
	private static final String INPUT_FILE = "/home/magic/Downloads/CA_DRU_proj_2010-2060.csv";

	public static class PopulationMapper extends
			Mapper<Object, Text, Text, LongWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			DemographyEntry demographyEntry = CSVFileHandler.read(value
					.toString());
			if (demographyEntry == null) {
				return;
			}

			if (demographyEntry.getYear() != 2016) {
				return;
			}
			if (demographyEntry.getAge() > 65) {
				context.write(new Text(demographyEntry.getCounty()),
						new LongWritable(demographyEntry.getPopulation()));
			}
		}
	}

	public static class PopulationReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {

		public void reduce(Text term, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable longWritable : values) {
				sum += longWritable.get();
			}
			context.write(term, new LongWritable(sum));
			System.out.print(term);
			System.out.print(",");
			System.out.println(sum);
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Aging Population County Calulator");
		job.setJarByClass(PopulationPlotMR.class);
		job.setMapperClass(PopulationMapper.class);
		job.setReducerClass(PopulationReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		// FileInputFormat.addInputPath(job, new Path(INPUT_FILE));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// FileOutputFormat.setOutputPath(job, new Path("target/output10"));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}

}
