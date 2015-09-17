package Tests;

import java.util.*;
import java.io.*;



import christen.FeatureGen_MR;
import christen.Parameters;




public class SimulateMR1 {

	HashMap<String,HashSet<String>> context;
	ArrayList<String> data;
	
	//constructor serves as mapper. context will get populated, 
	//and reducer will get called in final statement of constructor
	//Some changes compared to original program:
	// 1.converts line to lowercase 3. trims token and checks for emptiness
	public SimulateMR1(String dataset, String output)throws IOException{
		Scanner in=new Scanner(new FileReader(new File(dataset)));
		data=new ArrayList<String>();
		context=new HashMap<String,HashSet<String>>();
		int index=0;
		while(in.hasNextLine()){
			String line=in.nextLine();
			
			data.add(new String(line));
			line=line.toLowerCase();
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
					String word=new String(tokens[j]+","+i);
					String v=new String(line);
					if(!context.containsKey(word))
						context.put(word, new HashSet<String>());
					context.get(word).add(v+"::"+index);
					//System.out.println(v+";"+index);
				}
			  }
			  index++;
		}
		
		in.close();
		//printBlocks("bel-air hotel");
		Reducer(output);
	}
	
	public void Reducer(String output)throws IOException{
		
		HashMap<Double, ArrayList<String>> struct=new HashMap<Double,ArrayList<String>>();
		for(String key:context.keySet()){
			ArrayList<String> tuples=new ArrayList<String>(context.get(key));
			ArrayList<Integer> index=new ArrayList<Integer>(tuples.size());
			for(int i=0; i<tuples.size(); i++){
				
				String[] m=tuples.get(i).split("::");
				//System.out.println(m+"\n"+m[0]+" "+m[1]);
				tuples.set(i,m[0]);
				index.add(Integer.parseInt(m[1]));
				
			}
			
			
			FeatureGen_MR f=null;
			if(tuples.size()<=1)
				continue;
			else
				f=new FeatureGen_MR(tuples);
			
			ArrayList<ArrayList<Integer>> dupFeatures=f.getFeatures('d');
			ArrayList<ArrayList<Integer>> nondupFeatures=f.getFeatures('n');
			ArrayList<Double> dup_scores=f.getScores('d');
			ArrayList<Double> nondup_scores=f.getScores('n');
			ArrayList<Integer> dups1=f.getIndex1('d');
			ArrayList<Integer> dups2=f.getIndex2('d');
			ArrayList<Integer> nondups1=f.getIndex1('n');
			ArrayList<Integer> nondups2=f.getIndex2('n');		

			for(int i=0; i<dup_scores.size(); i++){
				String v=new String(convertToString(dupFeatures.get(i)));
		        //out.println(dup_scores.get(i)+"\t"+v+"\t"+tuples.get(dups1.get(i))+":"+tuples.get(dups2.get(i)));
				if(!struct.containsKey(dup_scores.get(i))){
					struct.put(dup_scores.get(i),new ArrayList<String>());
				}
				struct.get(dup_scores.get(i)).add(v);
				struct.get(dup_scores.get(i)).add(index.get(dups1.get(i))+" "+index.get(dups2.get(i)));
				struct.get(dup_scores.get(i)).add(tuples.get(dups1.get(i)));
				struct.get(dup_scores.get(i)).add(tuples.get(dups2.get(i)));
				
				
			}
			for(int i=0; i<nondup_scores.size(); i++){
				String v=new String(convertToString(nondupFeatures.get(i)));
		        //out.println(nondup_scores.get(i)+"\t"+v+"\t"+tuples.get(nondups1.get(i))+":"+tuples.get(nondups2.get(i)));
				if(!struct.containsKey(nondup_scores.get(i))){
					struct.put(nondup_scores.get(i),new ArrayList<String>());
				}
				struct.get(nondup_scores.get(i)).add(v);
				struct.get(nondup_scores.get(i)).add(index.get(nondups1.get(i))+" "+index.get(nondups2.get(i)));
				struct.get(nondup_scores.get(i)).add(tuples.get(nondups1.get(i)));
				struct.get(nondup_scores.get(i)).add(tuples.get(nondups2.get(i)));
				
				
				
			}
			
			
			
		}
		PrintWriter out=new PrintWriter(new File(output));
		ArrayList<Double> keys=new ArrayList<Double>(struct.keySet());
		Collections.sort(keys);
		for(int i=keys.size()-1; i>=0; i--)
			for(int j=0; j<struct.get(keys.get(i)).size(); j+=4){
				out.println(keys.get(i)+"\t"+struct.get(keys.get(i)).get(j));
				out.println(struct.get(keys.get(i)).get(j+1));
				out.println(struct.get(keys.get(i)).get(j+2));
				out.println(struct.get(keys.get(i)).get(j+3));
			}
		out.close();
		
	}
	
	private static String convertToString(ArrayList<Integer> feature){

	    String res="";
	    for(int i=0; i<feature.size(); i++)
	    	res+=(feature.get(i)+" ");
	    return res.substring(0,res.length()-1);

	   }
	
	@SuppressWarnings("unused")
	private void printBlocks(String val){
		
		for(String m:context.keySet()){
			if(hashSetContainsString(context.get(m),val)){
			System.out.println("Blocking Key "+m);
			for(String j:context.get(m))
				System.out.println(j);
			System.out.println();
			}
		}
	}
	
	private boolean hashSetContainsString(HashSet<String> hash, String string){
		for(String p:hash)
			if(p.contains(string))
				return true;
		return false;
	}
	
	public static void main(String[] args) throws IOException {
		
		
		
		testSets("restaurant");
		
	}

	public static void testSets(String dataset)throws IOException{
		if(dataset.equals("cora"))
			new SimulateMR1("/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/cora/coraRecords.csv","/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/cora/simulateMR1");
		else if(dataset.equals("restaurant"))
			new SimulateMR1("/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/restaurant/restaurantRecords.csv","/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/restaurant/simulateMR1");
		else if(dataset.equals("data1000"))
			new SimulateMR1("/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/data1000/data1000.csv","/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/data1000/simulateMR1");
		else if(dataset.equals("data2500"))
			new SimulateMR1("/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/data2500/data2500.csv","/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/data2500/simulateMR1");
		else if(dataset.equals("data5000"))
			new SimulateMR1("/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/data5000/data5000.csv","/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/data5000/simulateMR1");
		else if(dataset.equals("data10000"))
			new SimulateMR1("/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/data10000/data10000.csv","/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/data10000/simulateMR1");
		else if(dataset.equals("dataC10000"))
			new SimulateMR1("/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/dataC10000/data10000.csv","/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/dataC10000/simulateMR1");
		else if(dataset.equals("dataC5000"))
			new SimulateMR1("/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/dataC5000/data5000.csv","/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/dataC5000/simulateMR1");
		else if(dataset.equals("dataC2500"))
			new SimulateMR1("/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/dataC2500/data2500.csv","/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/dataC2500/simulateMR1");
		else if(dataset.equals("dataC1000"))
			new SimulateMR1("/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/dataC1000/data1000.csv","/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/dataC1000/simulateMR1");
		else if(dataset.equals("cloud"))
			new SimulateMR1("/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/cloud/cloud.csv","/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/cloud/simulateMR1");
			
	}
}
