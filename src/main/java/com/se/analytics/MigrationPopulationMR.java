package com.se.analytics;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

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

public class MigrationPopulationMR {
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
			context.write(
					new Text(demographyEntry.getYear() + ","
							+ demographyEntry.getAge() + ","
							+ demographyEntry.getGender() + ","),
					new LongWritable(demographyEntry.getPopulation()));
		}
	}

	public static class PopulationReducer extends
			Reducer<Text, LongWritable, Text, LongWritable> {

		public void reduce(Text term, Iterable<LongWritable> values,
				Context context) throws IOException, InterruptedException {

			long sum = 0;
			for (LongWritable text : values) {
				sum += text.get();
			}

			context.write(term, new LongWritable(sum));
			return;
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("DemographyFilter", args[2]);
		Job job = Job.getInstance(conf, "County's Median Population");
		job.setJarByClass(MigrationPopulationMR.class);
		job.setMapperClass(PopulationMapper.class);
		job.setReducerClass(PopulationReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		// FileInputFormat.addInputPath(job, new Path(INPUT_FILE));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// FileOutputFormat.setOutputPath(job, new Path("target/output10"));
		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// dd/MM/yyyy
		Date now = new Date();
		String strDate = sdfDate.format(now);
		FileOutputFormat.setOutputPath(job, new Path(args[1] + '_' + strDate));
		job.waitForCompletion(true);
	}

}
