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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import au.com.bytecode.opencsv.CSVParser;

public class KMedoids {

	public static enum GLOBAL_COUNTER {
		center_changed,
	};

	public static class KMedoidsMapper extends Mapper<Object, Text, Text, Text> {

		private ArrayList<String> centerList;
		private HashMap<String, Double> matrix;
		CSVParser parser = new CSVParser();

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			if (centerList != null)
				centerList.clear();
			centerList = null;
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Path[] uris = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			centerList = new ArrayList<String>();

			BufferedReader readBuffer1 = new BufferedReader(new FileReader(
					uris[0].toString()));
			String line;
			while ((line = readBuffer1.readLine()) != null) {
				centerList.add(line.replace("\n", "").replace("\r", ""));
			}
			readBuffer1.close();

			matrix = new HashMap<String, Double>();
			BufferedReader readBuffer2 = new BufferedReader(new FileReader(
					uris[1].toString()));
			String info[];
			while ((line = readBuffer2.readLine()) != null) {
				info = parser.parseLine(line.toString());
				matrix.put(info[0] + "," + info[1], Double.parseDouble(info[2]));
			}
			readBuffer2.close();
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String airport = value.toString();
			airport = airport.replace("\n", "").replace("\r", "");

			String minCenter = "";
			double minDis = Double.MAX_VALUE;
			for (String center : centerList) {
				if (center.equalsIgnoreCase(airport))
					continue;
				String matrixKey = center.compareTo(airport) < 0 ? center + ","
						+ airport : airport + "," + center;
				double dis = matrix.containsKey(matrixKey) ? matrix
						.get(matrixKey) : 0;
				if (dis <= minDis) {
					minDis = dis;
					minCenter = center;
				}
			}

			context.write(new Text(minCenter), new Text(airport));
		}
	}

	public static class KMedoidsReducer extends
			Reducer<Text, Text, Text, NullWritable> {
		private ArrayList<String> centerList;
		private HashMap<String, Double> matrix;
		CSVParser parser = new CSVParser();

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			if (centerList != null)
				centerList.clear();
			centerList = null;
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Path[] uris = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			centerList = new ArrayList<String>();

			BufferedReader readBuffer1 = new BufferedReader(new FileReader(
					uris[0].toString()));
			String line;
			while ((line = readBuffer1.readLine()) != null) {
				centerList.add(line.replace("\n", "").replace("\r", ""));
			}
			readBuffer1.close();

			matrix = new HashMap<String, Double>();
			BufferedReader readBuffer2 = new BufferedReader(new FileReader(
					uris[1].toString()));
			String info[];
			while ((line = readBuffer2.readLine()) != null) {
				info = parser.parseLine(line.toString());
				matrix.put(info[0] + "," + info[1], Double.parseDouble(info[2]));
			}
			readBuffer2.close();
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> airports = new ArrayList<String>();
			airports.add(key.toString());
			for (Text a : values)
				airports.add(a.toString());

			double minCost = Double.MAX_VALUE;
			int minIndex = -1;
			for (int i = 0; i < airports.size(); i++) {
				double cost = 0;
				for (int j = 0; j < airports.size(); j++) {
					if (i == j)
						continue;
					String matrixKey = airports.get(i).compareTo(
							airports.get(j)) < 0 ? airports.get(i) + ","
							+ airports.get(j) : airports.get(j) + ","
							+ airports.get(i);
					double dis = matrix.containsKey(matrixKey) ? matrix
							.get(matrixKey) : 0;
					cost += dis;
				}
				if (cost <= minCost) {
					minCost = cost;
					minIndex = i;
				}
			}
			if (minIndex != 0) {
				context.getCounter(GLOBAL_COUNTER.center_changed).increment(1);
			}

			context.write(new Text(airports.get(minIndex)), NullWritable.get());
		}
	}

	private static boolean RunKMedoidsJob(Configuration conf1, String input,
			String output, String centerFile, String matrix)
			throws IOException, InterruptedException, ClassNotFoundException,
			URISyntaxException {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "CourseProject-KMedoids");
		job.setJarByClass(KMedoids.class);
		job.setMapperClass(KMedoidsMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(KMedoidsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(10);

		Configuration conf2 = job.getConfiguration();
		DistributedCache.addCacheFile(new URI(centerFile), conf2);
		DistributedCache.addCacheFile(new URI(matrix), conf2);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
		long changed = job.getCounters()
				.findCounter(GLOBAL_COUNTER.center_changed).getValue();
		return changed == 0;
	}

	public static void run(Configuration conf, String matrix,
			String inputCenters, String airportList) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		String mapInput = airportList;
		String reduceOutput = airportList + "Xcenters/centers0";
		String centerFile = inputCenters;
		int i = 0;
		while(true)
		{
			System.out.println("Iteration " + i);
			if (RunKMedoidsJob(conf, mapInput, reduceOutput, centerFile, matrix))
				break;
			centerFile = reduceOutput;
			reduceOutput = reduceOutput.substring(0, reduceOutput.length()-1) + i;
			i++;
			break;
		}
	}
}
