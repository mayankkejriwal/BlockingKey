package MapReduce;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

import christen.Parameters;

import FeatureSelection.FeatureAnalysis;
import FeatureSelection.Fisher;
import FeatureSelection.LearnDisjunct;

//import libsvm.svm;

public class PostProcessing1 {

	public static void generateLabelsFile(ArrayList<ArrayList<Integer>> dupFeatures, 
			ArrayList<ArrayList<Integer>> nondupFeatures, String outfile)throws IOException{
		
		PrintWriter out=new PrintWriter(new File(outfile));
		
		for(int i=0; i<dupFeatures.size(); i++){
			String q=new String("");
			for(int j=0; j<dupFeatures.get(i).size(); j++){
				q+=((j+1)+":"+dupFeatures.get(i).get(j)+" ");
			}
			q=q.substring(0,q.length()-1);
			out.println("+1 "+q);
		}
		for(int i=0; i<nondupFeatures.size(); i++){
			String q=new String("");
			for(int j=0; j<nondupFeatures.get(i).size(); j++){
				q+=((j+1)+":"+nondupFeatures.get(i).get(j)+" ");
			}
			q=q.substring(0,q.length()-1);
			out.println("-1 "+q);
		}
		
		out.close();
	}
	public static void generateBKFile(ArrayList<ArrayList<Integer>> dupFeatures, 
			ArrayList<ArrayList<Integer>> nondupFeatures, String outfile)throws IOException{
		Fisher c=new Fisher(dupFeatures.get(0).size(),dupFeatures,nondupFeatures);
		c.computeStatistics();
		FeatureAnalysis d=new FeatureAnalysis(c);
		
		//compute  Blocking Keys
		LearnDisjunct e=null;
		int num_atts=dupFeatures.get(0).size()/Parameters.num_feats;
		
		
		
		e=new LearnDisjunct(d, 1, 2, num_atts, Parameters.recall);
		ArrayList<String> codes=null;
		if(Parameters.DNF){
			e.populateDNF_Features(Parameters.conjuncts);
			codes=e.codesDNF();
		}else{
			e.populateDisjunction_Features();
			codes=e.codes();
		}
		//print to file
		PrintWriter out=new PrintWriter(new File(outfile));
		for(String a:codes)
			out.println(a);
		
		out.close();
	}
	
	public static String generateBKFile(ArrayList<ArrayList<Integer>> dupFeatures, 
			ArrayList<ArrayList<Integer>> nondupFeatures)throws IOException{
		String res="";
		Fisher c=new Fisher(dupFeatures.get(0).size(),dupFeatures,nondupFeatures);
		c.computeStatistics();
		FeatureAnalysis d=new FeatureAnalysis(c);
		
		//compute  Blocking Keys
		LearnDisjunct e=null;
		int num_atts=dupFeatures.get(0).size()/Parameters.num_feats;
		
		
		
		e=new LearnDisjunct(d, 1, 2, num_atts, Parameters.recall);
		ArrayList<String> codes=null;
		if(Parameters.DNF){
			e.populateDNF_Features(Parameters.conjuncts);
			codes=e.codesDNF();
		}else{
			e.populateDisjunction_Features();
			codes=e.codes();
		}
		//print to file
		
		for(int i=0; i<codes.size()-1; i++)
			res+=(codes.get(i)+"\t");
		
		res=res+codes.get(codes.size()-1);
		
		return res;
		
	}
	
	//Double feature values not currently supported for this function
	public static void generateBKFile(String infile, double ut, double lt
			, int num_d, int num_nd, double recall, double eta, boolean DNF 
			,boolean use_all, String outfile)throws IOException{
		
		Scanner in =new Scanner(new FileReader(new File(infile)));
		HashMap<Double, HashSet<ArrayList<Integer>>> score_index_d=new HashMap<Double, HashSet<ArrayList<Integer>>>();
		HashMap<Double, HashSet<ArrayList<Integer>>> score_index_nd=new HashMap<Double, HashSet<ArrayList<Integer>>>();
		if(use_all){
			num_d=Integer.MAX_VALUE;
			num_nd=Integer.MAX_VALUE;
		}
		
		while(in.hasNextLine()){
			String line=in.nextLine();
			String[] tokens=line.split("\t");
			String[] feat=tokens[1].split(" ");
			double score=Double.parseDouble(tokens[0]);
			if(score>=ut){
				if(score_index_d.keySet().size()<num_d){
					if(!score_index_d.containsKey(score))
						score_index_d.put(score, new HashSet<ArrayList<Integer>>());
					score_index_d.get(score).add(convertToArrayListInteger(feat));
					
				}
				else{
					ArrayList<Double> p=new ArrayList<Double>(score_index_d.keySet());
					Collections.sort(p);
					if(p.get(0)<score)
						score_index_d.remove(p.get(0));
					if(!score_index_d.containsKey(score))
						score_index_d.put(score, new HashSet<ArrayList<Integer>>());
					score_index_d.get(score).add(convertToArrayListInteger(feat));
				}
			}
			else if(score<=lt && score>0.0){
				if(score_index_nd.keySet().size()<num_nd){
					if(!score_index_nd.containsKey(score))
						score_index_nd.put(score, new HashSet<ArrayList<Integer>>());
					score_index_nd.get(score).add(convertToArrayListInteger(feat));
					
				}
				else{
					ArrayList<Double> p=new ArrayList<Double>(score_index_nd.keySet());
					Collections.sort(p);
					if(p.get(p.size()-1)>score)
						score_index_nd.remove(p.get(p.size()-1));
					if(!score_index_nd.containsKey(score))
						score_index_nd.put(score, new HashSet<ArrayList<Integer>>());
					score_index_nd.get(score).add(convertToArrayListInteger(feat));
				}
			}
		
		}
		
		in.close();
		ArrayList<ArrayList<Integer>> dupFeatures=new ArrayList<ArrayList<Integer>>();
		ArrayList<ArrayList<Integer>> nondupFeatures=new ArrayList<ArrayList<Integer>>();
		//duplicates
				int count_d=0;
				ArrayList<Double> scores=new ArrayList<Double>(score_index_d.keySet());
				Collections.sort(scores);
				for(int i=scores.size()-1; i>=0 && count_d<num_d; i--){
					
					for(ArrayList<Integer> q: score_index_d.get(scores.get(i)))
						dupFeatures.add(q);
						
					
					count_d+=score_index_d.get(scores.get(i)).size();
				}
				
				//non_duplicates
				int count_nd=0;
				 scores=new ArrayList<Double>(score_index_nd.keySet());
				Collections.sort(scores);
				for(int i=0; i<score_index_nd.size() && count_nd<num_nd; i++){
					
					
					for(ArrayList<Integer> q: score_index_nd.get(scores.get(i)))
						nondupFeatures.add(q);
					
					count_nd+=score_index_nd.get(scores.get(i)).size();
				}
				
				//now compute the FeatureAnalysis DS
				Fisher c=new Fisher(dupFeatures.get(0).size(),dupFeatures,nondupFeatures);
				c.computeStatistics();
				FeatureAnalysis d=new FeatureAnalysis(c);
				
				//compute  Blocking Keys
				LearnDisjunct e=null;
				int num_atts=dupFeatures.get(0).size()/Parameters.num_feats;
				
				
				Parameters.eta=eta;
				Parameters.DNF=DNF;
				e=new LearnDisjunct(d, 1, 2, num_atts, recall);
				ArrayList<String> codes=null;
				if(DNF){
					e.populateDNF_Features(Parameters.conjuncts);
					codes=e.codesDNF();
				}else{
					e.populateDisjunction_Features();
					codes=e.codes();
				}
				//print to file
				PrintWriter out=new PrintWriter(new File(outfile));
				for(String a:codes)
					out.println(a);
				
				out.close();
				
	}
	public static void generateLabelsFile(String infile, double ut, double lt, 
			int num_d, int num_nd, String outfile) throws IOException{
		
		Scanner in =new Scanner(new FileReader(new File(infile)));
		HashMap<Double, HashSet<ArrayList<Double>>> score_index_d=new HashMap<Double, HashSet<ArrayList<Double>>>();
		HashMap<Double, HashSet<ArrayList<Double>>> score_index_nd=new HashMap<Double, HashSet<ArrayList<Double>>>();
		
		while(in.hasNextLine()){
			String line=in.nextLine();
			String[] tokens=line.split("\t");
			String[] feat=tokens[1].split(" ");
			double score=Double.parseDouble(tokens[0]);
			if(score>=ut){
				if(score_index_d.keySet().size()<num_d){
					if(!score_index_d.containsKey(score))
						score_index_d.put(score, new HashSet<ArrayList<Double>>());
					score_index_d.get(score).add(convertToArrayListDouble(feat));
					
				}
				else{
					ArrayList<Double> p=new ArrayList<Double>(score_index_d.keySet());
					Collections.sort(p);
					if(p.get(0)<score)
						score_index_d.remove(p.get(0));
					if(!score_index_d.containsKey(score))
						score_index_d.put(score, new HashSet<ArrayList<Double>>());
					score_index_d.get(score).add(convertToArrayListDouble(feat));
				}
			}
			else if(score<=lt && score>0.0){
				if(score_index_nd.keySet().size()<num_nd){
					if(!score_index_nd.containsKey(score))
						score_index_nd.put(score, new HashSet<ArrayList<Double>>());
					score_index_nd.get(score).add(convertToArrayListDouble(feat));
					
				}
				else{
					ArrayList<Double> p=new ArrayList<Double>(score_index_nd.keySet());
					Collections.sort(p);
					if(p.get(p.size()-1)>score)
						score_index_nd.remove(p.get(p.size()-1));
					if(!score_index_nd.containsKey(score))
						score_index_nd.put(score, new HashSet<ArrayList<Double>>());
					score_index_nd.get(score).add(convertToArrayListDouble(feat));
				}
			}
			
		}
		in.close();
		
		//now write it out to file
		
		PrintWriter out=new PrintWriter(new File(outfile));
		//duplicates
		int count_d=0;
		ArrayList<Double> scores=new ArrayList<Double>(score_index_d.keySet());
		Collections.sort(scores);
		for(int i=scores.size()-1; i>=0 && count_d<num_d; i--){
			
			for(ArrayList<Double> q: score_index_d.get(scores.get(i))){
				out.print("+1 ");
				printArrayListNodes(out, q);
				out.println();
			}
			count_d+=score_index_d.get(scores.get(i)).size();
		}
		
		//non_duplicates
		int count_nd=0;
		 scores=new ArrayList<Double>(score_index_nd.keySet());
		Collections.sort(scores);
		for(int i=0; i<score_index_nd.size() && count_nd<num_nd; i++){
			
			for(ArrayList<Double> q: score_index_nd.get(scores.get(i))){
				out.print("-1 ");
				printArrayListNodes(out, q);
				out.println();
			}
			count_nd+=score_index_nd.get(scores.get(i)).size();
		}
		
		out.close();
		
	}
	
	private static void printArrayListNodes(PrintWriter out, ArrayList<Double> nodes)throws IOException{
		for(int i=0; i<nodes.size()-1; i++)
			out.print((i+1)+":"+nodes.get(i)+" ");
		out.print(nodes.size()+":"+nodes.get(nodes.size()-1));
	}
	
	private static ArrayList<Double> convertToArrayListDouble(String[] feat) {
		ArrayList<Double> res=new ArrayList<Double>();
		for(int i=0; i<feat.length; i++)
			res.add(new Double(Double.parseDouble(feat[i])));
		return res;
	}

	private static ArrayList<Integer> convertToArrayListInteger(String[] feat) {
		ArrayList<Integer> res=new ArrayList<Integer>();
		for(int i=0; i<feat.length; i++)
			res.add(new Integer(Integer.parseInt(feat[i])));
		return res;
	}
	
	public static int checkScoreCount(String infile, double score, boolean upper)throws IOException{
		Scanner in =new Scanner(new FileReader(new File(infile)));
		int count=0;
		while(in.hasNextLine()){
			String line=in.nextLine();
			String[] tokens=line.split("\t");
			
			double sc=Double.parseDouble(tokens[0]);
			if(upper)
				if(sc>=score)
					count++;
				else
					;
			else
				if(sc<=score && sc>0.0)
					count++;
					
		}
		in.close();
		return count;
	}
	
	public static void main(String[] args)throws IOException{
		//System.out.println(checkScoreCount("/home/mayankkejriwal/Downloads/hadoop_programs/restaurantMR_1",0.1,true));
		
		generateBKFile("/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/restaurant/simulateMR1"
				, 0.07, 0.0025, 400, 1000, 
				0.9, 0.1, false, false,
				"/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/restaurant/simulateBK");
	
	}
}
