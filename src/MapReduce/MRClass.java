package MapReduce;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

import christen.FeatureGen_MR;
import christen.Parameters;
import christen.Unsupervised;

import FeatureSelection.FeatureAnalysis;
import FeatureSelection.Fisher;

public class MRClass {

	
	
	public static FeatureGen_MR generateFeatures(ArrayList<String> tuples) throws IOException{
		int att=tuples.get(0).split(",").length;
		
		FeatureGen_MR b=null;
		
			Unsupervised a=new Unsupervised(tuples,att);
			b=new FeatureGen_MR(a,att*Parameters.num_feats);
		
		
		
		b.setUpFeatures();
		return b;
	}
	
	public static void checkMRResults(String file) throws IOException{
		Scanner in =new Scanner(new FileReader(new File(file)));
		while(in.hasNextLine()){
			String line=in.nextLine();
			
			String[] tokens=line.split("\t");
			String[] feat=tokens[1].split(" ");
			printArray(feat);
			System.out.println(feat.length);
			return;
		}
		in.close();
	}
	
	private static void printArray(String[] arr){
		System.out.println();
		for(int i=0; i<arr.length; i++)
			System.out.print(arr[i]+" ");
	}
	
	public static FeatureAnalysis getMRResults(String file, char dataset)throws IOException{
		Scanner in =new Scanner(new FileReader(new File(file)));
		
		double ut=Parameters.ut;
		double lt=Parameters.lt;
		int d=Parameters.dupDemanded;
		int nd=Parameters.nondupDemanded;
		int feats=-1;
		ArrayList<ArrayList<Integer>> dupFeatures=new ArrayList<ArrayList<Integer>>(d);
		ArrayList<ArrayList<Integer>> nondupFeatures=new ArrayList<ArrayList<Integer>>(nd);
		
		ArrayList<String> lines=new ArrayList<String>(160000);
		while(in.hasNextLine()){
			String line=in.nextLine();
			lines.add(line);
		}
		in.close();
		if(lines.size()!=0)
			feats=lines.get(0).split("\t")[1].split(" ").length;
		System.out.println("File reading done "+lines.size());
		for(String line: lines){
			String[] tokens=line.split("\t");
			String[] feat=tokens[1].split(" ");
			if(Double.parseDouble(tokens[0])>=ut){
				dupFeatures.add(convertToArrayList(feat));
			}else if(Double.parseDouble(tokens[0])<=lt&&Double.parseDouble(tokens[0])>0.0){
				nondupFeatures.add(convertToArrayList(feat));
			}
		}
		
		Fisher c=new Fisher(feats,dupFeatures,nondupFeatures);
		System.out.println("computing statistics "+feats+" "+dupFeatures.get(0).size());
		c.computeStatistics();
		return new FeatureAnalysis(c);
	}
	
	private static ArrayList<Integer> convertToArrayList(String[] feat) {
		ArrayList<Integer> res=new ArrayList<Integer>();
		for(int i=0; i<feat.length; i++)
			res.add(new Integer(Integer.parseInt(feat[i])));
		return res;
	}
	
	public static void main(String[] args)throws IOException{
		checkMRResults("/home/mayankkejriwal/Downloads/hadoop_programs/cora_MR");
	
	}

}
