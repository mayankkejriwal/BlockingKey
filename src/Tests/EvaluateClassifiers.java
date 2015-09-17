package Tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

import MapReduce.PostProcessing1;

import christen.FeatureGen_MR;

import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;

public class EvaluateClassifiers {
	svm_model model;
	ImportGoldStandard gold;
	
	double mean;
	double deviation;
	
	
	//use for svm
	public EvaluateClassifiers(String modelFile, ImportGoldStandard gold)throws IOException{
		this.gold=gold;
		model=svm.svm_load_model(modelFile);
	}
	
	//use for feature summing, threshold
	public EvaluateClassifiers(ImportGoldStandard gold, double...statistics){
		this.gold=gold;
		mean=statistics[0];
		deviation=statistics[1];
	}
	
	public void printBlockMetrics_featureSum(HashMap<Integer,HashSet<Integer>> pairs){
		int[] count=new int[3];
		int[] truth=new int[3];// record FN, FP1 and FP2 resp.
		//FP1 (probable) truth value is treated as duplicate for this calc.
		for(int i:pairs.keySet())
			for(int j:pairs.get(i)){
				int res=classify_featureSum(i,j);
				count[res]++;
				if(gold.contains(i,j)&&res==0)
					truth[0]++;
				else if(!gold.contains(i,j)&&res==1)
					truth[1]++;
				else if(!gold.contains(i,j)&&res==2)
					truth[2]++;
			}
		double recall2=(count[2]-truth[2])*100.00/gold.num_dups;
		double recall1=(count[1]-truth[1])*100.00/gold.num_dups;
		double precision2=(count[2]-truth[2])*100.00/(count[2]);
		double precision1=(count[1]-truth[1])*100.00/(count[1]);
		System.out.println("Measure \t Dup \t Probable-Dup");
		System.out.println("Recall \t"+recall2+"\t"+recall1);
		System.out.println("Precision \t"+precision2+"\t"+precision1);
		
	}
	
	public void printGoldMetrics_featureSum(){
		int[] count=new int[3];
		if(gold.cluster_gold){
			for(int i=0; i<gold.clusters.size(); i++){
				ArrayList<Integer> p=new ArrayList<Integer>(gold.clusters.get(i));
				for(int j=0; j<p.size()-1; j++)
					for(int k=j+1; k<p.size(); k++)
						count[classify_featureSum(p.get(j),p.get(k))]++;
			}
				
		}
		else{
			for(int i:gold.duplicate_records.keySet())
				count[classify_featureSum(i,gold.duplicate_records.get(i))]++;
		}
		for(int i=0; i<=count.length-1; i++)
			System.out.println("Percentage of Category "+i+" is "+count[i]*100.00/gold.num_dups);
			
		
		
	}
	
	//return 0,1,2 in increasing order of duplicate probability
	public int classify_featureSum(int index1, int index2){
		String tuple1=gold.data.getTuples().get(index1);
		String tuple2=gold.data.getTuples().get(index2);
		ArrayList<Integer> FV=FeatureGen_MR.getFeatureWeights(tuple1, tuple2);
		int sum=0;
		for(int i=0; i<FV.size(); i++)
			sum+=FV.get(i);
		if(sum>=mean-deviation)
			return 2;
		else if(sum>=mean-2*deviation)
			return 1;
		
		else return 0;
		
	}
	
	public void printRandomNDPrecision(int limit){
		int[] count=new int[3];
		for(int i=0; i<limit; i++){
			Random p=new Random(3432524+limit);
			int q1=p.nextInt(gold.data.corpussize);
			int q2=p.nextInt(gold.data.corpussize);
			if(q1==q2||gold.contains(q1,q2))
			{
				i--;
				continue;
			}
			count[classify_featureSum(q1,q2)]++;
				
		}
		for(int i=0; i<=count.length-1; i++)
			System.out.println("Percentage of Category "+i+" is "+count[i]*100.00/limit);
		
	}
	
	public void printGoldMetrics_svm(){
		int count=0;
		if(gold.cluster_gold){
			for(int i=0; i<gold.clusters.size(); i++){
				ArrayList<Integer> p=new ArrayList<Integer>(gold.clusters.get(i));
				for(int j=0; j<p.size()-1; j++)
					for(int k=j+1; k<p.size(); k++)
						if(classify_svm(p.get(j),p.get(k)))
							count++;
			}
				
		}
		else{
			for(int i:gold.duplicate_records.keySet())
				if(classify_svm(i,gold.duplicate_records.get(i)))
					count++;
		}
		System.out.println(count);
		System.out.println("Gold Precision: "+(count*1.0/gold.num_dups));
		
	}
	
	private boolean classify_svm(int index1, int index2){
		String tuple1=gold.data.getTuples().get(index1);
		String tuple2=gold.data.getTuples().get(index2);
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
	
	
	public static void main(String[] args) throws IOException{
		testBlockResults("restaurant");
		System.out.println("Now blocking C");
		testBlockResults("dataC1000");
	}
	
	public static void testBlockResults(String dataset)throws IOException{
		if(dataset.equals("cora")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/cora/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"coraRecords.csv",prefix+"GoldStandard.csv",1);
			//evaluate generate examples
			EvaluateMR1 m=new EvaluateMR1(data, prefix+"simulateMR1");
			m.generatePrunedFeatureSets();
			EvaluateBK s=new EvaluateBK(prefix+"BK",data);
			EvaluateClassifiers k=new EvaluateClassifiers(data,m.printStatistics());
			k.printBlockMetrics_featureSum(s.return_pairs());
			
		}
		else if(dataset.equals("restaurant")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/restaurant/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"restaurantRecords.csv",prefix+"GoldStandard.csv",1);
			//evaluate generate examples
			EvaluateMR1 m=new EvaluateMR1(data, prefix+"simulateMR1");
			m.generatePrunedFeatureSets();
			EvaluateBK s=new EvaluateBK(prefix+"BK",data);
			EvaluateClassifiers k=new EvaluateClassifiers(data,m.printStatistics());
			k.printBlockMetrics_featureSum(s.return_pairs());
			
		}
		else if(dataset.equals("data1000")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/data1000/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data1000.csv",prefix+"GoldStandard.csv",0);
			//evaluate generate examples
			EvaluateMR1 m=new EvaluateMR1(data, prefix+"simulateMR1");
			m.generatePrunedFeatureSets();
			EvaluateBK s=new EvaluateBK(prefix+"BK",data);
			EvaluateClassifiers k=new EvaluateClassifiers(data,m.printStatistics());
			k.printBlockMetrics_featureSum(s.return_pairs());
			
		}
		else if(dataset.equals("data2500")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/data2500/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data2500.csv",prefix+"GoldStandard.csv",0);
			//evaluate generate examples
			EvaluateMR1 m=new EvaluateMR1(data, prefix+"simulateMR1");
			m.generatePrunedFeatureSets();
			EvaluateBK s=new EvaluateBK(prefix+"BK",data);
			EvaluateClassifiers k=new EvaluateClassifiers(data,m.printStatistics());
			k.printBlockMetrics_featureSum(s.return_pairs());
			
		}
		else if(dataset.equals("data5000")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/data5000/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data5000.csv",prefix+"GoldStandard.csv",0);
			//evaluate generate examples
			EvaluateMR1 m=new EvaluateMR1(data, prefix+"simulateMR1");
			m.generatePrunedFeatureSets();
			EvaluateBK s=new EvaluateBK(prefix+"BK",data);
			EvaluateClassifiers k=new EvaluateClassifiers(data,m.printStatistics());
			k.printBlockMetrics_featureSum(s.return_pairs());
			
		}
		else if(dataset.equals("data10000")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/data10000/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data10000.csv",prefix+"GoldStandard.csv",0);
			//evaluate generate examples
			EvaluateMR1 m=new EvaluateMR1(data, prefix+"simulateMR1");
			m.generatePrunedFeatureSets();
			EvaluateBK s=new EvaluateBK(prefix+"BK",data);
			EvaluateClassifiers k=new EvaluateClassifiers(data,m.printStatistics());
			k.printBlockMetrics_featureSum(s.return_pairs());
			
		}
		else if(dataset.equals("dataC5000")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/dataC5000/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data5000.csv",prefix+"GoldStandard.csv",1);
			//evaluate generate examples
			EvaluateMR1 m=new EvaluateMR1(data, prefix+"simulateMR1");
			m.generatePrunedFeatureSets();
			EvaluateBK s=new EvaluateBK(prefix+"BK",data);
			EvaluateClassifiers k=new EvaluateClassifiers(data,m.printStatistics());
			k.printBlockMetrics_featureSum(s.return_pairs());
			
		}
		else if(dataset.equals("dataC10000")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/dataC10000/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data10000.csv",prefix+"GoldStandard.csv",1);
			//evaluate generate examples
			EvaluateMR1 m=new EvaluateMR1(data, prefix+"simulateMR1");
			m.generatePrunedFeatureSets();
			EvaluateBK s=new EvaluateBK(prefix+"BK",data);
			EvaluateClassifiers k=new EvaluateClassifiers(data,m.printStatistics());
			k.printBlockMetrics_featureSum(s.return_pairs());
			
		}
		else if(dataset.equals("dataC1000")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/dataC1000/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data1000.csv",prefix+"GoldStandard.csv",1);
			//evaluate generate examples
			EvaluateMR1 m=new EvaluateMR1(data, prefix+"simulateMR1");
			m.generatePrunedFeatureSets();
			EvaluateBK s=new EvaluateBK(prefix+"BK",data);
			EvaluateClassifiers k=new EvaluateClassifiers(data,m.printStatistics());
			k.printBlockMetrics_featureSum(s.return_pairs());
			
		}
		else if(dataset.equals("dataC2500")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/dataC2500/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data2500.csv",prefix+"GoldStandard.csv",1);
			//evaluate generate examples
			EvaluateMR1 m=new EvaluateMR1(data, prefix+"simulateMR1");
			m.generatePrunedFeatureSets();
			EvaluateBK s=new EvaluateBK(prefix+"BK",data);
			EvaluateClassifiers k=new EvaluateClassifiers(data,m.printStatistics());
			k.printBlockMetrics_featureSum(s.return_pairs());
			
		}
	}
	
	public static void testAgainstGold(String dataset) throws IOException{
		if(dataset.equals("cora")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/cora/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"coraRecords.csv",prefix+"GoldStandard.csv",1);
			//evaluate generate examples
			EvaluateMR1 m=new EvaluateMR1(data, prefix+"simulateMR1");
			m.generatePrunedFeatureSets();
			
			EvaluateClassifiers k=new EvaluateClassifiers(data,m.printStatistics());
			k.printGoldMetrics_featureSum();
			k.printRandomNDPrecision(10000);
		}
		else if(dataset.equals("data1000")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/data1000/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data1000.csv",prefix+"GoldStandard.csv",0);
			//evaluate generate examples
			EvaluateMR1 m=new EvaluateMR1(data, prefix+"simulateMR1");
			m.generatePrunedFeatureSets();
			
			EvaluateClassifiers k=new EvaluateClassifiers(data,m.printStatistics());
			k.printGoldMetrics_featureSum();
			k.printRandomNDPrecision(10000);
		}
		else if(dataset.equals("data2500")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/data2500/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data2500.csv",prefix+"GoldStandard.csv",0);
			//evaluate generate examples
			EvaluateMR1 m=new EvaluateMR1(data, prefix+"simulateMR1");
			m.generatePrunedFeatureSets();
			
			EvaluateClassifiers k=new EvaluateClassifiers(data,m.printStatistics());
			k.printGoldMetrics_featureSum();
			k.printRandomNDPrecision(10000);
		}
		else if(dataset.equals("data5000")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/data5000/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data5000.csv",prefix+"GoldStandard.csv",0);
			//evaluate generate examples
			EvaluateMR1 m=new EvaluateMR1(data, prefix+"simulateMR1");
			m.generatePrunedFeatureSets();
			

			EvaluateClassifiers k=new EvaluateClassifiers(data,m.printStatistics());
			k.printGoldMetrics_featureSum();
			k.printRandomNDPrecision(10000);
		}
		else if(dataset.equals("data10000")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/data10000/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data10000.csv",prefix+"GoldStandard.csv",0);
			//evaluate generate examples
			EvaluateMR1 m=new EvaluateMR1(data, prefix+"simulateMR1");
			m.generatePrunedFeatureSets();
			

			EvaluateClassifiers k=new EvaluateClassifiers(data,m.printStatistics());
			k.printGoldMetrics_featureSum();
			k.printRandomNDPrecision(10000);
		}
		else if(dataset.equals("dataC10000")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/dataC10000/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data10000.csv",prefix+"GoldStandard.csv",1);
			//evaluate generate examples
			EvaluateMR1 m=new EvaluateMR1(data, prefix+"simulateMR1");
			m.generatePrunedFeatureSets();
			

			EvaluateClassifiers k=new EvaluateClassifiers(data,m.printStatistics());
			k.printGoldMetrics_featureSum();
			k.printRandomNDPrecision(10000);
		}
		else if(dataset.equals("dataC5000")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/dataC5000/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data5000.csv",prefix+"GoldStandard.csv",1);
			//evaluate generate examples
			EvaluateMR1 m=new EvaluateMR1(data, prefix+"simulateMR1");
			m.generatePrunedFeatureSets();
			

			EvaluateClassifiers k=new EvaluateClassifiers(data,m.printStatistics());
			k.printGoldMetrics_featureSum();
			k.printRandomNDPrecision(10000);
		}
		else if(dataset.equals("dataC2500")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/dataC2500/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data2500.csv",prefix+"GoldStandard.csv",1);
			//evaluate generate examples
			EvaluateMR1 m=new EvaluateMR1(data, prefix+"simulateMR1");
			m.generatePrunedFeatureSets();
			

			EvaluateClassifiers k=new EvaluateClassifiers(data,m.printStatistics());
			k.printGoldMetrics_featureSum();
			k.printRandomNDPrecision(10000);
		}
		else if(dataset.equals("dataC1000")){
			String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/dataC1000/";
			ImportGoldStandard data=new ImportGoldStandard(prefix+"data1000.csv",prefix+"GoldStandard.csv",1);
			//evaluate generate examples
			EvaluateMR1 m=new EvaluateMR1(data, prefix+"simulateMR1");
			m.generatePrunedFeatureSets();
			

			EvaluateClassifiers k=new EvaluateClassifiers(data,m.printStatistics());
			k.printGoldMetrics_featureSum();
			k.printRandomNDPrecision(10000);
		}
	}

}
