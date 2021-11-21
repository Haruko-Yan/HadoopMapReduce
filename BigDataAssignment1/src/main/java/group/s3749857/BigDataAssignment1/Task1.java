package group.s3749857.BigDataAssignment1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class Task1 extends Configured implements Tool{
	
	private static final Logger LOG = Logger.getLogger(Task1.class);
	
	public static class Mapper1 extends Mapper<LongWritable,Text,Text,IntWritable> {
		
		private static final Logger LOG = Logger.getLogger(Mapper1.class);
		private final static IntWritable one = new IntWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			// Set log-level to debugging
			LOG.setLevel(Level.DEBUG);
			
			LOG.debug("The mapper task of Ziqing Yan, s3749857");
			
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			String token;
			
			// Traverse the input words
			while (tokenizer.hasMoreElements()) {
				token = tokenizer.nextToken();
				// Transform the corresponding key according the length of the word
				if (token.length() >= 1 && token.length() <= 4) {
					context.write(new Text("Short words"), one);
				}
				else if (token.length() >= 5 && token.length() <= 7) {
					context.write(new Text("Medium words"), one);
				}
				else if (token.length() >= 8 && token.length() <= 10) {
					context.write(new Text("Long words"), one);
				}
				else if (token.length() > 10) {
					context.write(new Text("Extra-long words"), one);
				}
				else {
					LOG.error("Error in extracting words");
				}
			}
		}
	}
	
	public static class Partitioner1 extends Partitioner<Text, IntWritable> {

		private static final Logger LOG = Logger.getLogger(Partitioner1.class);
		
		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			// Set log-level to debugging
		    LOG.setLevel(Level.DEBUG);
		    
		    LOG.debug("The partitioner task of Ziqing Yan, s3749857");
		    
		    String keyString = key.toString();
		    
		    if (numPartitions == 0) {
		    	LOG.debug("No partitioning - only ONE reducer");
		    	return 0;
			}
		    // The key-value pair corresponding short words and medium words will be directed to Partitioner 0
		    if (keyString.equals("Short words") || keyString.equals("Medium words")) {
				return 0;
			}
		    // The key-value pair corresponding long words will be directed to Partitioner 1
		    else if (keyString.equals("Long words")) {
				return 1 % numPartitions;
			}
		    // The key-value pair corresponding extra-long words will be directed to Partitioner 2
		    else {
				return 2 % numPartitions;
			}
			
		}
		
	}
	
	public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private static final Logger LOG = Logger.getLogger(Reducer1.class);
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
    	System.out.println("Started running Task1 job.");
    	// Get the configuration that is set by command line
    	Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Task1");
        
        job.setJarByClass(Task1.class);
        
  		// Set input and output path entered by command line
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
  		
        // Set log-level to information
        LOG.setLevel(Level.INFO);
        
    	// Log all the arguments passed to the application
        LOG.info("Input path: " + args[0]);
        LOG.info("Output path: " + args[1]);
        
        // Set the tasks
        job.setMapperClass(Mapper1.class);
        job.setPartitionerClass(Partitioner1.class);
        job.setReducerClass(Reducer1.class);
        
        // Set the type of the output of key and value from mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
  		// Set the number of reduce tasks
        job.setNumReduceTasks(3);
        
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
        System.exit(ToolRunner.run(new Task1(), args));
    }

}
