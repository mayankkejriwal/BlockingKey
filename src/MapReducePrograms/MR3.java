package MapReducePrograms;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import christen.DNFBlocking_MR;
import christen.FeatureGen_MR;
import christen.Parameters;

public class MR3 {

public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    
    	
    	private Text word = new Text();
    	private Text v = new Text();    
    	
    	//private boolean DNF=false;
    	ArrayList<Integer> codes;
    	ArrayList<Integer> attributes;
    	ArrayList<ArrayList<Integer>> DNFcodes;
    	ArrayList<ArrayList<Integer>> DNFattributes;
    	
    	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          
		Path[] cacheFiles =DistributedCache.getLocalCacheFiles(context.getConfiguration());
		String q=new String("");
		
		FileInputStream in = new FileInputStream(cacheFiles[0].toString());	
		while(true){
			int s=in.read();
			if(s==-1)
				break;
			q+=((char) s);
		}
		
		in.close();
		String[] bk=(q.split(System.getProperty("line.separator"))[2].split("\t"));
		
			
		ArrayList<String> BKs=new ArrayList<String>();
		for(int i=1; i<bk.length; i++)
			BKs.add(bk[i]);
		
		
		String line = value.toString().toLowerCase();
		  String[] res=line.split("\"");
		  if(res.length>1){
		   	String total="";
			for(int j=1; j<res.length; j+=2)
				res[j]=res[j].replace(",","");
			for(int p=0; p<res.length; p++)
				total+=res[p];
			line=total;
					
		  }
		  
		  v.set(line);
		  DNFBlocking_MR block=new DNFBlocking_MR(BKs,line);
		  for(int i=0; i<block.codes_DNF.size(); i++){
			  String bkv=block.block(i);
			  word.set(bkv);
			  context.write(word,v);
			  
		  }
		
		
		
	  
 	}
 } 
        
 public static class Reduce extends Reducer<Text, Text, Text, Text> {
	
	 private Text u=new Text();
    private Text v=new Text();
    private double mean;
    private double dev;
    
    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        
    	//storing mean and dev
    	Path[] cacheFiles =DistributedCache.getLocalCacheFiles(context.getConfiguration());
		String q=new String("");
		
		FileInputStream in = new FileInputStream(cacheFiles[0].toString());	
		while(true){
			int s=in.read();
			if(s==-1)
				break;
			q+=((char) s);
		}
		
		in.close();
		mean=Double.parseDouble(q.split(System.getProperty("line.separator"))[0].split("\t")[1]);
		dev=Double.parseDouble(q.split(System.getProperty("line.separator"))[1].split("\t")[1]);
		
		
    	
    	
       
    	
		ArrayList<String> tuples=new ArrayList<String>();
        for (Text val : values) {
        	tuples.add(val.toString());
	  
        }
        
        for(int i=0; i<tuples.size()-1; i++){
        	for(int j=i+1; j<tuples.size()&&j<i+Parameters.c; j++){
        		ArrayList<Integer> FV=FeatureGen_MR.getFeatureWeights(tuples.get(i), tuples.get(j));
        		int sum=0;
        		for(int k=0; k<FV.size(); k++)
        			sum+=FV.get(k);
        		if(sum>=mean)
        			{
        			u.set("High "+Integer.toString(sum));
        			v.set(tuples.get(i)+"\t"+tuples.get(j));
        			context.write(u,v);
        			}
        		else if(sum>=mean-dev)
        			{
        				u.set("Probable "+Integer.toString(sum));
        				v.set(tuples.get(i)+"\t"+tuples.get(j));
        				context.write(u,v);
        			}
        		
        		
        	}
        }
    	
  
		}
    

 }
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	        
	    Job job = new Job(conf, "mr3");
	    
	    DistributedCache.addCacheFile(new URI(args[0]),job.getConfiguration());
	    
	    job.setJarByClass(MR3.class);
	    
	    
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	        
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	        
	    job.waitForCompletion(true);
	 }
}
