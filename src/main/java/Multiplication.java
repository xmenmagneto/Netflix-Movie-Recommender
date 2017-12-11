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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Multiplication {
	public static class MultiplicationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

		Map<Integer, List<MovieRelation>> movieRelationMap = new HashMap<Integer,List<MovieRelation>>();

		@Override
		protected void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			String filePath = conf.get("coOccurrencePath", "/coOccurrenceMatrix/part-r-00000");
			Path pt = new Path(filePath);
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = br.readLine();
			
			while(line != null) {
				//movieA:movieB \t relation
				String[] tokens = line.toString().trim().split("\t");
				String[] movies = tokens[0].split(":");

				int movie1 = Integer.parseInt(movies[0]);
				int movie2 = Integer.parseInt(movies[1]);
				int relation = Integer.parseInt(tokens[2]);

				MovieRelation movieRelation = new MovieRelation(movie1, movie2, relation);
				if (movieRelationMap.containsKey(movie1)) {
					movieRelationMap.get(movie1).add(movieRelation);
				} else {
					List<MovieRelation> list = new ArrayList<MovieRelation>();
					list.add(movieRelation);
					movieRelationMap.put(movie1, list);
				}
			}
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


		}
	}

	public static class MultiplicationReducer extends Reducer<Text, DoubleWritable, IntWritable, Text> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("coOccurrencePath", args[0]);
		
		Job job = Job.getInstance();
		job.setMapperClass(MultiplicationMapper.class);
		job.setReducerClass(MultiplicationReducer.class);
		
		job.setJarByClass(Multiplication.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		TextInputFormat.setInputPaths(job, new Path(args[1]));
		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
}
