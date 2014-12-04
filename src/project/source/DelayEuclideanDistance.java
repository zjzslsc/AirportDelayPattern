package project.source;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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

		private ArrayList<String> airportList;

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			if (airportList != null)
				airportList.clear();
			airportList = null;
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Path[] uris = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			airportList = new ArrayList<String>();

			BufferedReader readBuffer1 = new BufferedReader(new FileReader(
					uris[0].toString()));
			String line;
			while ((line = readBuffer1.readLine()) != null) {
				airportList.add(line.replace("\n", "").replace("\r", ""));
			}
			readBuffer1.close();

		}

		CSVParser parser = new CSVParser();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// ArrayList<String> airportDelays = new ArrayList<String>();
			HashMap<String, Double> airportDelays = new HashMap<String, Double>();

			for (Text avgDelay : values) {
				String[] info = parser.parseLine(avgDelay.toString());
				String airport = info[0];
				double delay = Double.parseDouble(info[1]);
				airportDelays.put(airport, new Double(delay));
			}

			for (int i = 0; i < airportList.size(); i++) {
				for (int j = i + 1; j < airportList.size(); j++) {
					if (i == j)
						continue;
					String airport1 = airportList.get(i);
					String airport2 = airportList.get(j);
					double delay1 = airportDelays.containsKey(airport1) ? airportDelays
							.get(airport1) : 0;
					double delay2 = airportDelays.containsKey(airport2) ? airportDelays
							.get(airport2) : 0;
					if (delay1 == 0 && delay2 == 0)
						continue;
					int cmp = airport1.compareToIgnoreCase(airport2);
					//double distance = Math.pow(delay1 - delay2, 2);
					double distance = Math.abs(delay1 - delay2);
					String distancePair = "";
					if (cmp == 0) // won't happen, keep the compiler happy
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

	public static void RunEuclideanDistanceJob(Configuration conf,
			String input, String output, String airportFile)
			throws IOException, InterruptedException, ClassNotFoundException,
			URISyntaxException {

		Job job = new Job(conf, "CourseProject-DelayEuclideanDistance");
		job.setJarByClass(DelayEuclideanDistance.class);
		job.setMapperClass(DistanceMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(DistanceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(10);

		Configuration conf2 = job.getConfiguration();
		DistributedCache.addCacheFile(new URI(airportFile), conf2);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		if (job.waitForCompletion(true)) {
		} else
			System.exit(1);
	}

}
