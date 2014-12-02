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
		/*
		 * Binary 000 represent 3 jobs now 001 is the preprocessing 010 is the
		 * euclidean distance 100 is the distance matrix
		 */
		if ((selection & 1) == 1)
			PreProcessing.RunPreProcessingJob(conf, otherArgs[0], otherArgs[1]);
		if ((selection & 2) == 2)
			DelayEuclideanDistance.RunEuclideanDistanceJob(conf, otherArgs[0],
					otherArgs[1]);
		if ((selection & 4) == 4)
			DistanceMatrix.RunMatrixDistanceJob(conf, otherArgs[0],
					otherArgs[1]);
	}
}
