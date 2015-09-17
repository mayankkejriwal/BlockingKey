package MapReduce;

import java.io.IOException;
import java.util.*;

import christen.FeatureGen_MR;
import christen.Parameters;
import libsvm.*;

public class ClassifierSVM {
	
	
	ArrayList<String> tuples;
	static svm_model model;
	HashMap<Integer, HashSet<Integer>> duplicates; //all integers index into tuples
	
	public ClassifierSVM(ArrayList<String> tuples) throws IOException{
		this.tuples=tuples;
		model=svm.svm_load_model(Parameters.SVM);
		duplicates=new HashMap<Integer, HashSet<Integer>>();
	}
	
	public void find_duplicates(){
		if(tuples.size()<=1)
			return;
		for(int i=0; i<tuples.size()-1; i++){
			ArrayList<String> tup_c=new ArrayList<String>(Parameters.c);
			for(int j=i; j<tuples.size() && j-i<Parameters.c; j++)
				tup_c.add(tuples.get(j));
			calcDupsInCBlock(tup_c, i);
		}
	}
	
	private void calcDupsInCBlock(ArrayList<String> cblock, int offset){
		String tuple1=cblock.get(0);
		for(int i=1; i<cblock.size(); i++){
			ArrayList<Integer> FV=FeatureGen_MR.getFeatureWeights(tuple1, cblock.get(i));
			svm_node[] nodes=new svm_node[FV.size()];
			for(int j=0; j<nodes.length; j++){
				nodes[j]=new svm_node();
				nodes[j].index=j;
				nodes[j].value= (double) FV.get(j);
			}
			int prediction=(int) svm.svm_predict(model, nodes);
			if(prediction>0){
				if(!duplicates.containsKey(offset))
					duplicates.put(offset, new HashSet<Integer>());
				duplicates.get(offset).add(i+offset);
			}
		}
			
		
	}
	
	public HashMap<Integer, HashSet<Integer>> getDuplicates(){
		return duplicates;
	}
}
