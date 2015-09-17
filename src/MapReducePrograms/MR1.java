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
import christen.*;
        
public class MR1 {
  
 

     
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    
    	
    	private Text word = new Text();
    	private Text v = new Text();    
    
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
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
	  String[] cols=line.split(",");
	  for(int i=0; i<cols.length; i++){
		String[] tokens=cols[i].split(Parameters.splitstring);
		for(int j=0; j<tokens.length; j++){
			if(tokens[j].trim().length()==0)
				continue;
			word.set(tokens[j]+","+i);
			v.set(line);
			context.write(word,v);
		}
	  }
 	}
 } 
        
 public static class Reduce extends Reducer<Text, Text, DoubleWritable, Text> {
	
    
    private Text v=new Text();
    
    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        ArrayList<String> tuples=new ArrayList<String>();
	
        for (Text val : values) {
            tuples.add(val.toString());
	  
        }
	if(tuples.size()>1){
	
		FeatureGen_MR f=new FeatureGen_MR(tuples);
		
		ArrayList<ArrayList<Integer>> dupFeatures=f.getFeatures('d');
		ArrayList<ArrayList<Integer>> nondupFeatures=f.getFeatures('n');
		ArrayList<Double> dup_scores=f.getScores('d');
		ArrayList<Double> nondup_scores=f.getScores('n');
		ArrayList<Integer> dups1=f.getIndex1('d');
		ArrayList<Integer> dups2=f.getIndex2('d');
		ArrayList<Integer> nondups1=f.getIndex1('n');
		ArrayList<Integer> nondups2=f.getIndex2('n');		

		for(int i=0; i<dup_scores.size(); i++){
			String feat=new String(convertToString(dupFeatures.get(i)));
	        
			//v.set(feat+System.getProperty("line.separator")+tuples.get(dups1.get(i))
				//	+System.getProperty("line.separator")+tuples.get(dups2.get(i)));
			
			v.set(feat+"\t"+tuples.get(dups1.get(i))
					+"\t"+tuples.get(dups2.get(i)));
			
			context.write(new DoubleWritable(dup_scores.get(i).doubleValue()),v);
			
			
		}
		for(int i=0; i<nondup_scores.size(); i++){
			String feat=new String(convertToString(nondupFeatures.get(i)));
	        
			//v.set(feat+System.getProperty("line.separator")+tuples.get(nondups1.get(i))
				//	+System.getProperty("line.separator")+tuples.get(nondups2.get(i)));
			
			v.set(feat+"\t"+tuples.get(nondups1.get(i))
					+"\t"+tuples.get(nondups2.get(i)));
			
			context.write(new DoubleWritable(nondup_scores.get(i).doubleValue()),v);
		}
			
	}
    }


    private static String convertToString(ArrayList<Integer> feature){

    String res="";
    for(int i=0; i<feature.size(); i++)
    	res+=(feature.get(i)+" ");
    return res.substring(0,res.length()-1);

   }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "mr1");
    job.setJarByClass(MR1.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(DoubleWritable.class);
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
