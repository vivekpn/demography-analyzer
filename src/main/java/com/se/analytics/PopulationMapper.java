package com.se.analytics;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PopulationMapper implements
		Mapper<Object, Text, Text, LongWritable> {

	@Override
	public void configure(JobConf arg0) {

	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public void map(Object arg0, Text value,
			OutputCollector<Text, LongWritable> context, Reporter arg3)
			throws IOException {
		try {
			String[] csvs = value.toString().split(",");
			int countyCode = Integer.parseInt(csvs[0]);
			String countyName = csvs[1];
			int year = Integer.parseInt(csvs[2]);
			int ethnicityCode = Integer.parseInt(csvs[3]);
			String ethnicity;
			String gender;
			int age;
			long population;
			if (csvs.length == 9) {
				ethnicity = csvs[4] + ',' + csvs[5];
				gender = csvs[6];
				age = Integer.parseInt(csvs[7]);
				population = Long.parseLong(csvs[8]);
			} else {
				ethnicity = csvs[4];
				gender = csvs[5];
				age = Integer.parseInt(csvs[6]);
				population = Long.parseLong(csvs[7]);
			}
			if (year != 2016) {
				return;
			}
			// if (age > 65) {
			context.collect(new Text(countyName), new LongWritable(population));
			// }
		} catch (NumberFormatException numberFormatException) {
		}

	}
}
