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

import project.source.DelayEuclideanDistance.DistanceMapper;
import project.source.DelayEuclideanDistance.DistanceReducer;
import au.com.bytecode.opencsv.CSVParser;

public class DistanceMatrix {
	public static class MatrixMapper extends
			Mapper<Object, Text, Text, DoubleWritable> {

		CSVParser parser = new CSVParser();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] data = parser.parseLine(value.toString());
			String emitKey = data[4] + "," + data[5];
			double emitValue = Double.parseDouble(data[6]);
			context.write(new Text(emitKey), new DoubleWritable(emitValue));
		}

	}

	public static class MatrixReducer extends
			Reducer<Text, DoubleWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			long airportDistance = 0;
			for (DoubleWritable dis : values) {
				airportDistance += dis.get();
			}
			double sqrtDis = Math.sqrt(airportDistance);
			context.write(
					new Text(key.toString() + ","
							+ String.valueOf(sqrtDis)),
					NullWritable.get());
		}
	}

	public static void RunMatrixDistanceJob(Configuration conf, String input,
			String output) throws IOException, InterruptedException,
			ClassNotFoundException {

		Job job = new Job(conf, "CourseProject-MatrixDistance");
		job.setJarByClass(DistanceMatrix.class);
		job.setMapperClass(MatrixMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setReducerClass(MatrixReducer.class);
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
