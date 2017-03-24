package com.se.analytics;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.gson.Gson;
import com.se.data.DemographyEntry;
import com.se.file.CSVFileHandler;

public class MedianPopulationMR {
	private static final String INPUT_FILE = "/home/magic/Downloads/CA_DRU_proj_2010-2060.csv";
	private static Gson gson = new Gson();

	public static class PopulationMapper extends
			Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String json = context.getConfiguration().get("DemographyFilter");
			DemographyEntry filter = gson.fromJson(json, DemographyEntry.class);
			DemographyEntry demographyEntry = CSVFileHandler.read(value
					.toString());
			if (demographyEntry == null) {
				return;
			}
			if (demographyEntry.filtered(filter)) {
				context.write(new Text(demographyEntry.getCounty() + '_'
						+ demographyEntry.getYear()),
						new Text(demographyEntry.getAge() + " \t "
								+ demographyEntry.getPopulation()));
			}

		}
	}

	public static class PopulationReducer extends
			Reducer<Text, Text, Text, LongWritable> {

		public void reduce(Text term, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			Map<Integer, Long> populations = new TreeMap<Integer, Long>();
			for (Text text : values) {
				StringTokenizer stringTokenizer = new StringTokenizer(
						text.toString());
				int age = Integer.parseInt(stringTokenizer.nextToken());
				long population = Long.parseLong(stringTokenizer.nextToken());
				if (populations.containsKey(age)) {
					populations.put(age, populations.get(age) + population);
				} else {
					populations.put(age, population);
				}
			}
			long sum = 0;
			for (Long population : populations.values()) {
				sum += population;
			}
			long cumulative = 0;
			for (Entry<Integer, Long> entry : populations.entrySet()) {
				cumulative += entry.getValue();
				if (cumulative > sum / 2) {
					context.write(term, new LongWritable(entry.getKey()));
					return;
				}
			}
			return;
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("DemographyFilter", args[2]);
		Job job = Job.getInstance(conf, "County's Median Population");
		job.setJarByClass(MedianPopulationMR.class);
		job.setMapperClass(PopulationMapper.class);
		job.setReducerClass(PopulationReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
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
