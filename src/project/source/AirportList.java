package project.source;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import project.source.DistanceMatrix.MatrixMapper;
import project.source.DistanceMatrix.MatrixReducer;
import au.com.bytecode.opencsv.CSVParser;

public class AirportList {
	public static class AirportMapper extends
			Mapper<Object, Text, Text, NullWritable> {
		CSVParser parser = new CSVParser();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] data = parser.parseLine(value.toString());
			String origin = data[PreProcessing.INDEX_ORIGIN];
			String dest = data[PreProcessing.INDEX_DEST];
			if (origin.equalsIgnoreCase("origin") || dest.equalsIgnoreCase("dest"))
				return;
			context.write(new Text(origin), NullWritable.get());
			context.write(new Text(dest), NullWritable.get());
		}
	}

	public static class AirportReducer extends
			Reducer<Text, NullWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static void RunAirportListJob(Configuration conf, String input,
			String output) throws IOException, InterruptedException,
			ClassNotFoundException {

		Job job = new Job(conf, "CourseProject-AirportList");
		job.setJarByClass(AirportList.class);
		job.setMapperClass(AirportMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setReducerClass(AirportReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(10);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		if (job.waitForCompletion(true)) {
		} else
			System.exit(1);
	}
}
