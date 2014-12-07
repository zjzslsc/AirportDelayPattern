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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import project.source.DelayEuclideanDistance.DistanceMapper;
import project.source.DelayEuclideanDistance.DistanceReducer;
import au.com.bytecode.opencsv.CSVParser;

public class KNN {

	private static final int K = 5000;

	public static class KNNMapper extends Mapper<Object, Text, Text, Text> {

		private ArrayList<String> airportList;
		private HashMap<String, Double> matrix;
		CSVParser parser = new CSVParser();

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

			matrix = new HashMap<String, Double>();
			BufferedReader readBuffer2 = new BufferedReader(new FileReader(
					uris[1].toString()));
			String info[];
			while ((line = readBuffer2.readLine()) != null) {
				info = parser.parseLine(line.toString());
				matrix.put(info[0] + "," + info[1], Double.parseDouble(info[2]));
			}
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String clusterInfo[] = parser.parseLine(value.toString());
			for (String toBeClassify : airportList) {
				int neighbors = 0;
				String hashKey = null;
				for (String classObj : clusterInfo) {
					hashKey = toBeClassify.compareTo(classObj) < 0 ? toBeClassify
							+ "," + classObj
							: classObj + "," + toBeClassify;
					if (matrix.containsKey(hashKey) && matrix.get(hashKey) <= K)
						neighbors++;
				}
				context.write(new Text(toBeClassify), new Text(value.toString()
						+ "-" + String.valueOf(neighbors)));
			}
		}
	}

	public static class KNNReducer extends
			Reducer<Text, Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String cluster = "Noise";
			int maxNeighbor = -1;
			for (Text info : values) {
				String c = info.toString().substring(0,
						info.toString().indexOf("-"));
				int neighbor = Integer.parseInt(info.toString().substring(
						info.toString().indexOf("-") + 1));
				if (neighbor > maxNeighbor) {
					maxNeighbor = neighbor;
					cluster = c;
				}
			}
			context.write(new Text(key.toString() + "--------[" + cluster.toString()+"]"),
					NullWritable.get());
		}
	}
	
	public static void RunKNNJob(Configuration conf,
			String input, String output, String toBeClassified, String matrix)
			throws IOException, InterruptedException, ClassNotFoundException,
			URISyntaxException {
	Job job = new Job(conf, "CourseProject-DelayEuclideanDistance");
	job.setJarByClass(KNN.class);
	job.setMapperClass(KNNMapper.class);

	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);

	job.setReducerClass(KNNReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(NullWritable.class);

	job.setNumReduceTasks(10);

	Configuration conf2 = job.getConfiguration();
	DistributedCache.addCacheFile(new URI(toBeClassified), conf2);
	DistributedCache.addCacheFile(new URI(matrix), conf2);

	FileInputFormat.addInputPath(job, new Path(input));
	FileOutputFormat.setOutputPath(job, new Path(output));
	if (job.waitForCompletion(true)) {
	} else
		System.exit(1);
	}
}
