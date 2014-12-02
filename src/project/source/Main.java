package project.source;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * Main entrance of the project program
 */
public class Main {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		//PreProcessing.RunPreProcessingJob(conf, otherArgs[0], otherArgs[1]);
		//DelayEuclideanDistance.RunEuclideanDistanceJob(conf, otherArgs[0], otherArgs[1]);
		DistanceMatrix.RunMatrixDistanceJob(conf, otherArgs[0], otherArgs[1]);
	}
}
