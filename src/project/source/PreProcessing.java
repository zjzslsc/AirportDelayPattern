package project.source;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import au.com.bytecode.opencsv.CSVParser;

/*
 * This procedure calculates the average weather delay for
 * each airport within 1 hour.
 */
public class PreProcessing {

	public static final int INDEX_YEAR = 0;
	public static final int INDEX_MONTH = 2;
	public static final int INDEX_DAY = 3;
	public static final int INDEX_ORIGIN = 14;
	public static final int INDEX_DEST = 23;
	public static final int INDEX_CRS_DEP_TIME = 29;
	public static final int INDEX_CRS_ARR_TIME = 40;
	public static final int INDEX_WEATHER_DELAY = 57;
	public static final int INDEX_CANCELLED = 47;
	public static final int INDEX_DIVERTED = 49;

	public static class ProcessingMapper extends
			Mapper<Object, Text, ProcessingIntermediumKey, DoubleWritable> {

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] flightData = new CSVParser().parseLine(value.toString());
			ProcessingIntermediumKey emitKeyDep = new ProcessingIntermediumKey();
			try {
				emitKeyDep.year = Integer.parseInt(flightData[INDEX_YEAR]);
				emitKeyDep.month = Integer.parseInt(flightData[INDEX_MONTH]);
				emitKeyDep.day = Integer.parseInt(flightData[INDEX_DAY]);
				emitKeyDep.time = Integer
						.parseInt(flightData[INDEX_CRS_DEP_TIME]);
			} catch (Exception e) {
				return;
			}
			emitKeyDep.airport = flightData[INDEX_ORIGIN];
			double weatherDelay = 0;
			try {
				weatherDelay = Double
						.parseDouble(flightData[INDEX_WEATHER_DELAY]);
			} catch (NumberFormatException e) {
				weatherDelay = 0;
			}
			context.write(emitKeyDep, new DoubleWritable(weatherDelay));

			ProcessingIntermediumKey emitKeyArr = new ProcessingIntermediumKey();
			try {
				emitKeyArr.year = Integer.parseInt(flightData[INDEX_YEAR]);
				emitKeyArr.month = Integer.parseInt(flightData[INDEX_MONTH]);
				emitKeyArr.day = Integer.parseInt(flightData[INDEX_DAY]);
				emitKeyArr.time = Integer
						.parseInt(flightData[INDEX_CRS_ARR_TIME]);
				emitKeyArr.airport = flightData[INDEX_DEST];
			} catch (Exception e) {
				return;
			}
			context.write(emitKeyArr, new DoubleWritable(weatherDelay));
		}
	}

	public static class ProcessingReducer
			extends
			Reducer<ProcessingIntermediumKey, DoubleWritable, Text, NullWritable> {

		public void reduce(ProcessingIntermediumKey key,
				Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {
			int hour = key.time / 100;
			double totalWeatherDelay = 0;
			double numDelay = 0;
			for (DoubleWritable delay : values) {
				totalWeatherDelay += delay.get();
				numDelay++;
			}
			double avgWeatherDelay = totalWeatherDelay / numDelay;
			String emitKey = "" + key.year + "," + key.month + "," + key.day
					+ "," + hour + "," + key.airport + "," + avgWeatherDelay;
			context.write(new Text(emitKey), NullWritable.get());
		}
	}

	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(ProcessingIntermediumKey.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2) {
			ProcessingIntermediumKey p1 = (ProcessingIntermediumKey) w1;
			ProcessingIntermediumKey p2 = (ProcessingIntermediumKey) w2;
			return p1.compareTo(p2);
		}
	}

	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(ProcessingIntermediumKey.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2) {
			ProcessingIntermediumKey p1 = (ProcessingIntermediumKey) w1;
			ProcessingIntermediumKey p2 = (ProcessingIntermediumKey) w2;

			if (p1.year != p2.year)
				return p1.year > p2.year ? 1 : -1;
			if (p1.month != p2.month)
				return p1.month > p2.month ? 1 : -1;
			if (p1.day != p2.day)
				return p1.day > p2.day ? -1 : 1;
			int cmp = p1.airport.compareTo(p2.airport);
			if (cmp != 0)
				return cmp;
			int p1hour = p1.time / 100;
			int p2hour = p2.time / 100;
			return p1hour > p2hour ? 1 : (p1hour == p2hour ? 0 : -1);
		}
	}

	public static void RunPreProcessingJob(Configuration conf, String input,
			String output) throws IOException, InterruptedException,
			ClassNotFoundException {

		Job job = new Job(conf, "CourseProject");
		job.setJarByClass(PreProcessing.class);
		job.setMapperClass(ProcessingMapper.class);

		job.setMapOutputKeyClass(ProcessingIntermediumKey.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setReducerClass(ProcessingReducer.class);
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
