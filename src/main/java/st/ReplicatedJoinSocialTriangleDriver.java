package st;

import java.io.BufferedReader;
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

		private Text outvalue = new Text();

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {

			URI[] cacheFiles = context.getCacheFiles();

			if (cacheFiles != null && cacheFiles.length > 0) {
				FileSystem fs = FileSystem.get(context.getConfiguration());
				Path path = new Path(cacheFiles[0].toString());
				BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));

				String line;
				while ((line = reader.readLine()) != null) {
					String[] field = line.split(",", -1);
					String user1_id = field[0];
					String user2_id = field[1];

					if (null != user1_id && user1_id.length() > 0 && null != user2_id && user2_id.length() > 0) {

						if (userId1TouserId2.containsKey(user1_id)) {

							ArrayList<String> followingList;
							followingList = userId1TouserId2.get(user1_id);
							followingList.add(user2_id);
						} else {
							//followingList.clear();
							ArrayList<String> list = new ArrayList<>();
							list.add(user2_id);
							userId1TouserId2.put(user1_id, list);
						}
					}
				}

				for (String key : userId1TouserId2.keySet()) {
					System.out.println("Key: " + key + "Value: "+userId1TouserId2.get(key));
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
			ArrayList<String> end;
			ArrayList<String> start1;

			int counter = 0;

			while (itr.hasMoreTokens()) {

				start = itr.nextToken();
				mid = itr.nextToken();

				if (userId1TouserId2.containsKey(mid)) {
					end = userId1TouserId2.get(mid);

					for (String t: end) {
						if (userId1TouserId2.containsKey(t)) {
							start1 = userId1TouserId2.get(t);

							if (start1.contains(start)) {
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
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");

		job.addCacheFile(new URI(args[0] + "/edges1.csv"));

		job.setMapperClass(TriangleCountMapper.class);
		job.setNumReduceTasks(0);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		val = job.waitForCompletion(true) ? 0 : 1;

		Counters counters = job.getCounters();
		Counter c1 = counters.findCounter(RESULT_COUNTER.finalCount);

		logger.info("TOTAL TRIANGLES COUNT: " + c1.getValue() / 3);

		return val;
	}


	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			throw new Error("Three arguments required:\n<input-dir> <output1-dir> <output2-dir>");
		}

		try {
			ToolRunner.run(new ReplicatedJoinSocialTriangleDriver(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}
