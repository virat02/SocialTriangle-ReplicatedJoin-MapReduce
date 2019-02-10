package st;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ReplicatedJoinSocialTriangleDriver extends Configured implements Tool {

	private static final Logger logger = LogManager.getLogger(ReplicatedJoinSocialTriangleDriver.class);

	//Global counters
	public enum RESULT_COUNTER {
		finalCount
	}

	public static class TriangleCountMapper extends
			Mapper<Object, Text, Text, Text> {

		private HashMap<String, ArrayList<String>> userId1TouserId2 = new HashMap<>();

		private int MAX = 30000;

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {

			//Retrieve from local cache
			URI[] cacheFiles = context.getCacheFiles();

			if (cacheFiles != null && cacheFiles.length > 0) {

				//Read input to a bufferedReader
				BufferedReader reader = new BufferedReader((new FileReader("edges.csv")));

				String line;
				while ((line = reader.readLine()) != null) {
					String[] field = line.split(",", -1);
					String user1_id = field[0];
					String user2_id = field[1];

					//filter out userId's below the threshold value "MAX"
					if ((Integer.parseInt(user1_id) <= MAX) && (Integer.parseInt(user2_id) <= MAX)) {

						//populate the hashmap
						if (userId1TouserId2.containsKey(user1_id)) {

							ArrayList<String> followingList = userId1TouserId2.get(user1_id);
							followingList.add(user2_id);

						} else {

							ArrayList<String> list = new ArrayList<>();
							list.add(user2_id);
							userId1TouserId2.put(user1_id, list);
						}
					}
				}

				reader.close();
			}
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			final StringTokenizer itr = new StringTokenizer(value.toString(), ",");

			String start;
			String mid;

			//Initialize counter to 0
			int counter = 0;

			while (itr.hasMoreTokens()) {

				start = itr.nextToken();
				mid = itr.nextToken();

				//if key exists, we know there is a path-2-length possible
				if (userId1TouserId2.containsKey(mid)) {

					//check if there exists a closing path
					for (String t: userId1TouserId2.get(mid)) {
						if (userId1TouserId2.containsKey(t)) {

							if (userId1TouserId2.get(t).contains(start)) {

								//increment counter since closing path exists, thereby forming a triangle
								counter++;
							}

						}
					}
				}
			}

			//set the final count globally
			context.getCounter(RESULT_COUNTER.finalCount).increment(counter);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {

		int val;
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Replicated Join SocialTriangle");
		job.setJarByClass(ReplicatedJoinSocialTriangleDriver.class);

		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "");

		//Add the input to cache file
		job.addCacheFile(new URI(args[0] + "/edges.csv"));

		//set the sequential flow of job
		job.setMapperClass(TriangleCountMapper.class);
		job.setNumReduceTasks(0);

		//Set the input path for job
		TextInputFormat.setInputPaths(job, new Path(args[0]));

		//Set the output path for job
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		//Set the output key and value type for this job
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		val = job.waitForCompletion(true) ? 0 : 1;

		//Get the count of total triangles using global counter
		Counters counters = job.getCounters();
		Counter c1 = counters.findCounter(RESULT_COUNTER.finalCount);

		//divide by 3, since (X,Y,Z), (Y,Z,X), (Z,X,Y) will all be counted individually
		logger.info("TOTAL TRIANGLES COUNT: " + c1.getValue() / 3);

		return val;
	}


	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output1-dir>");
		}

		try {
			ToolRunner.run(new ReplicatedJoinSocialTriangleDriver(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}
