package Tests;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

import christen.FeatureGen_MR;

import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;

//used for evaluating direct results of SVM (hence only for duplicates)
public class EvaluateSVM_old {

	svm_model model;
	ImportGoldStandard gold;
	private HashMap<String, HashSet<String>> duplicates;
	HashMap<Integer, HashSet<Integer>> results;
	
	public EvaluateSVM_old(String result_file, String svm_model, ImportGoldStandard gold)throws IOException{
		model=svm.svm_load_model(svm_model);
		this.gold=gold;
		
		duplicates=new HashMap<String,HashSet<String>>();
		Scanner in=new Scanner(new FileReader(result_file));
		while(in.hasNextLine()){
			String[] tokens=in.nextLine().split("\"");
			int flag=0;
			if(tokens.length!=4)
				System.out.println("ERROR "+tokens.length);
			if(duplicates.containsKey(tokens[1]))
				if(duplicates.get(tokens[1]).contains(tokens[3]))
					continue;
				else
					flag=1;
			else if(duplicates.containsKey(tokens[3]))
				if(duplicates.get(tokens[3]).contains(tokens[1]))
					continue;
				else
					flag=2;
			
			if(flag==1)
				duplicates.get(tokens[1]).add(tokens[3]);
			else if(flag==2)
				duplicates.get(tokens[3]).add(tokens[1]);
			else if(flag==0)
			{
				duplicates.put(tokens[1], new HashSet<String>());
				duplicates.get(tokens[1]).add(tokens[3]);
			}
			
		}
		in.close();
		build_results(true);
	}
	
	//will null duplicates to preserve memory if nullify true
	private void build_results(boolean nullify){
		ArrayList<String> tuples=gold.data.getTuples();
		results=new HashMap<Integer,HashSet<Integer>>();
		for(String tuple:duplicates.keySet()){
			int index1=tuples.indexOf(tuple);
			
			results.put(index1, new HashSet<Integer>());
			for(String tuple2:duplicates.get(tuple))
				results.get(index1).add(tuples.indexOf(tuple2));
		}
		if(nullify)
			duplicates=null;
	}
	
	public void print_metrics(){
		int true_pos=gold.duplicate_records.size();
		if(true_pos==0){
			System.out.println("No gold standard");
			return;
		}
		int total_count=0;
		int pos=0;
		for(int i:results.keySet()){
			total_count+=results.get(i).size();
			for(int j:results.get(i))
				if(gold.duplicate_records.containsKey(i))
					if((int) gold.duplicate_records.get(i)==j)
						pos++;
					else ;
				else if(gold.duplicate_records.containsKey(j))
					if((int) gold.duplicate_records.get(j)==i)
						pos++;
		}
		if(total_count==0){
			System.out.println("No precision metric possible: no duplicates");
			System.out.println("Recall: 0.0");
			return;
		}
		
		System.out.println("Recall: "+(double) pos/true_pos);
		System.out.println("Precision: "+(double) pos/total_count);
		
	}
	
	public void print_gold_metrics(){
		int count=0;
		ArrayList<String> tuples=gold.data.getTuples();
		for(int i:gold.duplicate_records.keySet())
			if(classify_svm(tuples.get(i),tuples.get(gold.duplicate_records.get(i))))
					count++;
		System.out.println("Gold Precision: "+(double) count/gold.duplicate_records.size());
		System.out.println(count+" "+gold.duplicate_records.size());
	}
	
	private boolean classify_svm(String tuple1, String tuple2){
		ArrayList<Integer> FV=FeatureGen_MR.getFeatureWeights(tuple1, tuple2);
		svm_node[] nodes=new svm_node[FV.size()];
		for(int j=0; j<nodes.length; j++){
			nodes[j]=new svm_node();
			nodes[j].index=j;
			nodes[j].value= (double) FV.get(j);
		}
		int prediction=(int) svm.svm_predict(model, nodes);
		
		if(prediction>0)
			return true;
		else
			return false;
	}
	
	public static void main(String[] args)throws IOException{
		String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/restaurant/";
		ImportGoldStandard restaurant=new ImportGoldStandard(prefix+"restaurantRecords.csv",prefix+"GoldStandard.csv",0);
		EvaluateSVM_old s=new EvaluateSVM_old(prefix+"restaurantMR2",prefix+"simulate.model",restaurant);
		
		s.print_metrics();
	}
}
