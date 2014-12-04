package project.source;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

/*
 * Main entrance of the project program
 * Takes 3 arguments <input folder>, <output folder>, <job selection>
 */
public class Main {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		int selection = Integer.parseInt(otherArgs[2]);

		if (selection == 0)
			AirportList.RunAirportListJob(conf, otherArgs[0], otherArgs[1]);
		if (selection == 1)
			PreProcessing.RunPreProcessingJob(conf, otherArgs[0], otherArgs[1]);
		// When calculating euclidean distance, we have a global knowleadge file
		// containing every aiport to pass in
		if (selection == 2)
			DelayEuclideanDistance.RunEuclideanDistanceJob(conf, otherArgs[0],
					otherArgs[1], otherArgs[3]);
		if (selection == 3)
			DistanceMatrix.RunMatrixDistanceJob(conf, otherArgs[0],
					otherArgs[1]);
	}
}
