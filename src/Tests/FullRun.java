package Tests;

import java.io.IOException;

import MapReduce.PostProcessing1;

public class FullRun {

	
	public static void main(String[] args) throws IOException {
		
		String inputFile="Enter the path of the csv file here";
		String weakOutput="Enter the path of a file to output weak training data";
		String BKFile="Enter the path of a file to which blocking keys will be output";
		String goldStandard="Enter the path to a gold standard file, to evaluate results";
		
		String prefix="/home/mayankkejriwal/Downloads/hadoop_programs/RecordLinkage/data2500/";
		inputFile=prefix+"data2500.csv";
		weakOutput=prefix+"weakOutput";
		BKFile=prefix+"BKFile";
		goldStandard=prefix+"GoldStandard.csv";
		
		ImportGoldStandard gold=new ImportGoldStandard(inputFile, goldStandard, 1);
		new SimulateMR1(inputFile, weakOutput); //first generate the weak training data in weakOutput
		System.out.println("Weak Training Data output");
		EvaluateMR1 m=new EvaluateMR1(null, weakOutput); //the next three steps will generate the blocking key
		m.generatePrunedFeatureSets();
		PostProcessing1.generateBKFile(m.prunedDupFeats, m.prunedNondupFeats, BKFile);
		System.out.println("Blocking Key determined");
		EvaluateBK s=new EvaluateBK(BKFile,gold); //the next two steps will return a simple Gaussian-based classifier
		EvaluateClassifiers k=new EvaluateClassifiers(gold,m.printStatistics());
		System.out.println("Classification complete. Beginning evaluation...");
		k.printBlockMetrics_featureSum(s.return_pairs()); //evaluate the results of the blocking key (return precision and recall)
		System.out.println("Evaluation complete. Terminated.");
	}

}
