package group.s3749857.BigDataAssignment1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class Task3 extends Configured implements Tool{

private static final Logger LOG = Logger.getLogger(Task3.class);
	
	public static class Mapper3 extends Mapper<LongWritable,Text,Text,IntWritable> {
		
		private static final Logger LOG = Logger.getLogger(Mapper3.class);
		
		// Initialize a Map for achieving in-mapper combining
		Map<String, Integer> map = new HashMap<String, Integer>();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			// Set log-level to debugging
			LOG.setLevel(Level.DEBUG);
			
			LOG.debug("The mapper task of Ziqing Yan, s3749857");

			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			String token;
			while (tokenizer.hasMoreElements()) {
				token = tokenizer.nextToken();
				if (map.containsKey(token)) {
					map.put(token, map.get(token) + 1);
				}
				else {
					map.put(token, 1);
				}
			}
		}
		
		// Use cleanup() method to transform all accumulated key-value pairs to Reducer in the end of the task
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// Traverse key-value pairs and transform them to Reducer
			for (Entry<String, Integer> entry : map.entrySet()) {
				context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
			}
		}
	}
	
	public static class Reducer3 extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private static final Logger LOG = Logger.getLogger(Reducer3.class);
		private IntWritable result = new IntWritable(); // the sum of the same type of words
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, 
		InterruptedException {
			// Set log-level to debugging
			LOG.setLevel(Level.DEBUG);
			
			LOG.debug("The reducer task of Ziqing Yan, s3749857");
			
			// initialize the sum
			int sum = 0; 
			for (IntWritable v : value) {
				sum += v.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public int run(String[] args) throws Exception {
    	System.out.println("Started running Task3 job.");
    	// Get the configuration that is set by command line
    	Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Task3");
        
        job.setJarByClass(Task3.class);
        
  		// Set input and output path entered by command line
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
  		
        // Set log-level to information
        LOG.setLevel(Level.INFO);
        
    	// Log all the arguments passed to the application
        LOG.info("Input path: " + args[0]);
        LOG.info("Output path: " + args[1]);
        
        // Set the tasks
        job.setMapperClass(Mapper3.class);
        job.setReducerClass(Reducer3.class);
        
        // Set the type of the output of key and value from mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        // Set input and output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // Set the type of the output of key and value from reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
  		
        System.exit(job.waitForCompletion(true)? 0 : 1);
        return 0;
    }
	
	public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Task3(), args));
    }
}
