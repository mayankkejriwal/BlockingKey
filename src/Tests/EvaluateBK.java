package Tests;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;

import christen.DNFBlocking_MR;

public class EvaluateBK {
	
	ImportGoldStandard gold;
	DNFBlocking_MR block_obj;
	
	private HashMap<String, HashSet<Integer>> blocks;
	
	public EvaluateBK(String BKFile, ImportGoldStandard gold)throws IOException{
		this.gold=gold;
		ArrayList<String> BKs=new ArrayList<String>();
		Scanner in=new Scanner(new FileReader(BKFile));
		while(in.hasNextLine())
			BKs.add(in.nextLine());
		block_obj=new DNFBlocking_MR(BKs, null);
		in.close();
		build_blocks();
	}
	
	private void build_blocks(){
		ArrayList<String> tuples=gold.data.getTuples();
		blocks=new HashMap<String,HashSet<Integer>>();
		for(int i=0; i<tuples.size(); i++){
			block_obj.setLine(tuples.get(i));
			//System.out.println(i);
			for(int j=0; j<block_obj.num_clauses; j++){
				
				String bk=block_obj.block(j);
				if(blocks.containsKey(bk))
					blocks.get(bk).add(i);
				else{
					blocks.put(bk, new HashSet<Integer>());
					blocks.get(bk).add(i);
				}
			}
		}
	}
	
	public HashMap<Integer,HashSet<Integer>> return_pairs(){
		HashMap<Integer, HashSet<Integer>> pairs=new HashMap<Integer, HashSet<Integer>>();
		for(String bk: blocks.keySet()){
			ArrayList<Integer> list=new ArrayList<Integer>(blocks.get(bk));
			Collections.sort(list);
			for(int i=0; i<list.size()-1; i++)
				for(int j=i+1; j<list.size(); j++)
					if(pairContains(pairs,list.get(i),list.get(j))==1){
						pairs.get(list.get(i)).add(list.get(j));
					}
					else if(pairContains(pairs,list.get(i),list.get(j))==0){
						pairs.put(list.get(i),new HashSet<Integer>());
						pairs.get(list.get(i)).add(list.get(j));
					}
		}
		return pairs;
	}
	//will print Pairs Completeness (recall) and Reduction Ratio
	//we will adopt the standard defn of reduction ratio, so pairs are only counted once
	public void print_metrics(){
		HashMap<Integer, HashSet<Integer>> pairs=return_pairs();
		int total=gold.data.getTuples().size();
		total=total*(total-1)/2;
		System.out.println("Reduction Ratio is: "+(1.0-(double) countHashMap(pairs)/total));
		int count=0;
		for(int i:pairs.keySet())
			for(int j:pairs.get(i))
			if(goldContains(i,j))
				count++;
		System.out.println("Pairs Completeness is: "+(double) count/gold.num_dups);
	}
	
	private int countHashMap(HashMap<Integer, HashSet<Integer>> pairs){
		int count=0;
		for(int i:pairs.keySet()){
			count+=pairs.get(i).size();
		}
		return count;
	}
	
	//k1 must be less than k2
	//returns 2 if pair is present, 1 if only k1 is present, 0 if neither
	private int pairContains(HashMap<Integer,HashSet<Integer>> pairs, int k1, int k2){
		
		
			if(pairs.containsKey(k1))
				if(pairs.get(k1).contains(k2))
					return 2;
				else
					return 1;
		
		return 0;
	}
	
	private boolean goldContains(int element1, int element2){
		if(gold.contains(element1, element2))
			return true;
		return false;
	}
	
	private boolean contains(ArrayList<Integer> index1, ArrayList<Integer> index2, int i, int j){
		for(int p=0; p<index1.size(); p++)
			if((int)index1.get(p)==i)
				if((int)index2.get(p)==j)
					return true;
				else
					continue;
			else if((int)index1.get(p)==j)
				if((int)index2.get(p)==i)
					return true;
				else
					continue;
		return false;
	}
	
	public static void main(String[] args)throws IOException{
		
			
			testSets("dataC1000");
		
	}
	
	public static void testSets(String dataset)throws IOException{
		if(dataset.equals("restaurant")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/restaurant/";
			ImportGoldStandard restaurant=new ImportGoldStandard(prefix+"restaurantRecords.csv",prefix+"GoldStandard.csv",0);
			EvaluateBK s=new EvaluateBK("/home/mayankkejriwal/Documents/datasets/restaurant/restaurantFeatures.csv",restaurant);
			s.print_metrics();
		}
		else if(dataset.equals("cora")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/cora/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"coraRecords.csv",prefix+"GoldStandard.csv",1);
			EvaluateBK s=new EvaluateBK(prefix+"BK",data);
			s.print_metrics();
		}
		else if(dataset.equals("data1000")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/data1000/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data1000.csv",prefix+"GoldStandard.csv",0);
			EvaluateBK s=new EvaluateBK(prefix+"BK",data);
			s.print_metrics();
		}
		else if(dataset.equals("data2500")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/data2500/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data2500.csv",prefix+"GoldStandard.csv",0);
			EvaluateBK s=new EvaluateBK(prefix+"BK",data);
			s.print_metrics();
		}
		else if(dataset.equals("data5000")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/data5000/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data5000.csv",prefix+"GoldStandard.csv",0);
			EvaluateBK s=new EvaluateBK(prefix+"BK",data);
			s.print_metrics();
		}
		else if(dataset.equals("data10000")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/data10000/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data10000.csv",prefix+"GoldStandard.csv",0);
			EvaluateBK s=new EvaluateBK(prefix+"BK",data);
			s.print_metrics();
		}
		else if(dataset.equals("dataC10000")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/dataC10000/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data10000.csv",prefix+"GoldStandard.csv",1);
			EvaluateBK s=new EvaluateBK(prefix+"BK",data);
			s.print_metrics();
		}
		else if(dataset.equals("dataC5000")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/dataC5000/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data5000.csv",prefix+"GoldStandard.csv",1);
			EvaluateBK s=new EvaluateBK(prefix+"BK",data);
			s.print_metrics();
		}
		else if(dataset.equals("dataC1000")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/dataC1000/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data1000.csv",prefix+"GoldStandard.csv",1);
			EvaluateBK s=new EvaluateBK(prefix+"BK",data);
			s.print_metrics();
		}
		else if(dataset.equals("dataC2500")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/dataC2500/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data2500.csv",prefix+"GoldStandard.csv",1);
			EvaluateBK s=new EvaluateBK(prefix+"BK",data);
			s.print_metrics();
		}
	}

}
