package MapReducePrograms;


import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import christen.Parameters;

import MapReduce.PostProcessing1;
import Tests.EvaluateMR1;
        
public class MR2 {
  
 

     
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    
    	
    	private Text word = new Text();
    	//private Text v = new Text();    
    
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          
			word.set("1");
			
			context.write(word,value);
		
	  
 	}
 } 
        
 public static class Reduce extends Reducer<Text, Text, Text, Text> {
	
	 private Text u=new Text();
    private Text v=new Text();
    
    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        ArrayList<String> tuples=new ArrayList<String>();
        HashSet<String> tuples1=new HashSet<String>();
	
        for (Text val : values) {
        	if(!tuples1.contains(val.toString())){
        		tuples.add(val.toString());
        		tuples1.add(val.toString());
        	}
	  
        }
        tuples1=null;
        EvaluateMR1 data=new EvaluateMR1(tuples);
        data.generatePrunedFeatureSets();
        double[] stats=data.printStatistics();
        String BK=PostProcessing1.generateBKFile(data.prunedDupFeats,data.prunedNondupFeats);
        
        u.set("Mean");
        v.set(Double.toString(stats[0]));
		context.write(u,v);
		
		u.set("Deviation");
        v.set(Double.toString(stats[1]));
		context.write(u,v);
		
		if(Parameters.DNF)
			u.set("DNF");
		else
			u.set("Disjunctive");
        v.set(BK);
		context.write(u,v);
		
			
		}
    


    
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "mr2");
    job.setJarByClass(MR2.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}

