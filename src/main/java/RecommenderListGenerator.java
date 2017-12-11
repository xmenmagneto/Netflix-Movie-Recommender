import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RecommenderListGenerator {
	public static class RecommenderListGeneratorMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		//filter out watched movies
		//match movie_name to movie_id

		@Override
		protected void setup(Context context) throws IOException {

		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		}
	}

	public static class RecommenderListGeneratorReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		@Override
		protected void setup(Context context) throws IOException {

		}

		// reduce method
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("watchHistory", args[0]);
		conf.set("movieTitles", args[1]);

		Job job = Job.getInstance(conf);
		job.setMapperClass(RecommenderListGeneratorMapper.class);
		job.setReducerClass(RecommenderListGeneratorReducer.class);

		job.setJarByClass(RecommenderListGenerator.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[2]));
		TextOutputFormat.setOutputPath(job, new Path(args[3]));

		job.waitForCompletion(true);
	}
}
