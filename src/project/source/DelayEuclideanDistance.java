package project.source;

import java.io.IOException;
import java.util.ArrayList;

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

import project.source.PreProcessing.GroupComparator;
import project.source.PreProcessing.KeyComparator;
import project.source.PreProcessing.ProcessingMapper;
import project.source.PreProcessing.ProcessingReducer;
import au.com.bytecode.opencsv.CSVParser;

public class DelayEuclideanDistance {

	public static class DistanceMapper extends Mapper<Object, Text, Text, Text> {

		CSVParser parser = new CSVParser();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] avgDelayData = parser.parseLine(value.toString());
			String emitKey = avgDelayData[0] + "," + avgDelayData[1] + ","
					+ avgDelayData[2] + "," + avgDelayData[3];
			String emitValue = avgDelayData[4] + "," + avgDelayData[5];
			context.write(new Text(emitKey), new Text(emitValue));
		}
	}

	public static class DistanceReducer extends
			Reducer<Text, Text, Text, NullWritable> {
		CSVParser parser = new CSVParser();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> airportDelays = new ArrayList<String>();
			for (Text avgDelay : values)
				airportDelays.add(avgDelay.toString());

			for (int i = 0; i < airportDelays.size(); i++) {
				String[] info1 = parser.parseLine(airportDelays.get(i));
				String airport1 = info1[0];
				double delay1 = Double.parseDouble(info1[1]);
				for (int j = i + 1; j < airportDelays.size(); j++) {
					String[] info2 = parser.parseLine(airportDelays.get(j));
					String airport2 = info2[0];
					double delay2 = Double.parseDouble(info2[1]);

					int cmp = airport1.compareToIgnoreCase(airport2);
					double distance = Math.pow(delay1 - delay2, 2);
					String distancePair = "";
					if (cmp == 0)
						continue;
					if (cmp > 0)
						distancePair = airport2 + "," + airport1 + ","
								+ String.valueOf(distance);
					if (cmp < 0)
						distancePair = airport1 + "," + airport2 + ","
								+ String.valueOf(distance);
					context.write(
							new Text(key.toString() + "," + distancePair),
							NullWritable.get());
				}
			}
		}
	}
	
	public static void RunEuclideanDistanceJob(Configuration conf, String input,
			String output) throws IOException, InterruptedException,
			ClassNotFoundException {

		Job job = new Job(conf, "CourseProject-DelayEuclideanDistance");
		job.setJarByClass(DelayEuclideanDistance.class);
		job.setMapperClass(DistanceMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(DistanceReducer.class);
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
