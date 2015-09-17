package christen;

import java.io.IOException;
import java.util.ArrayList;

public class FeatureGen_MR {

	
	
	ArrayList<Double> dup_scores;
	ArrayList<Double> nondup_scores;
	//dup1, dup2, nondup2, nondup2 are wrt to db.
	ArrayList<Integer> dup1;
	ArrayList<Integer> dup2;
	
	ArrayList<Integer> nondup1;
	ArrayList<Integer> nondup2;
	
	ArrayList<ArrayList<Integer>> dupFeatures;
	ArrayList<ArrayList<Integer>> nondupFeatures;
	
	int num_feats=-1; //total number of features
	int num_attr=-1;
	
	TFIDF db;
	
	
	
	
	
	public FeatureGen_MR(ArrayList<String> tuples){
		try {
			db=new TFIDF(tuples);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		num_attr=db.numatts;
		num_feats=num_attr*Parameters.num_feats;
		
		dup_scores=new ArrayList<Double>();
		nondup_scores=new ArrayList<Double>();
		dup1=new ArrayList<Integer>();
		dup2=new ArrayList<Integer>();
		nondup1=new ArrayList<Integer>();
		nondup2=new ArrayList<Integer>();
		for(int i=0; i<tuples.size()-1; i++)
			
			for(int j=i+1; j<Parameters.c && j< tuples.size(); j++){
				double score=db.getScoreTF(i,j);
				if(score>=Parameters.ut){
					dup_scores.add(score);
					dup1.add(i);
					dup2.add(j);
					
				}
				else if(score<=Parameters.lt && score>0.0)
				{
					nondup_scores.add(score);
					nondup1.add(i);
					nondup2.add(j);
				}
			}
		setUpFeatures();
	}
	
	
	public FeatureGen_MR(Unsupervised pop, int numfeats) throws IOException {
		
		db=pop.db;
		num_feats=numfeats;
		
		setUpTraining(pop);

	}
	
	
	
	
	
	
	public void setUpTraining(Unsupervised a) throws IOException{
		num_attr=a.numatts;
		a.computeDuplicates_optim(Parameters.dupDemanded,Parameters.nondupDemanded,Parameters.ut,Parameters.lt,null);
		dup_scores=a.dup_scores;
		nondup_scores=a.nondup_scores;
		dup1=a.dup1;
		dup2=a.dup2;
		nondup1=a.nondup1;
		nondup2=a.nondup2;
		a=null;
		
		
	}
	
	public void setUpFeatures(){
		dupFeatures=new ArrayList<ArrayList<Integer>>(dup1.size());
		nondupFeatures=new ArrayList<ArrayList<Integer>>(nondup1.size());
		
		//System.out.println("constructing duplicate features");
		for(int i=0; i<dup1.size(); i++)
			dupFeatures.add(getFeatureWeights_opt(i,0));
		
		//System.out.println("constructing non duplicate features");
		for(int i=0; i<nondup1.size(); i++)
			nondupFeatures.add(getFeatureWeights_opt(i,1));
		
		checkFeatureLength();
		
	}
	
	public ArrayList<ArrayList<Integer>> getFeatures(char opt){
		if(opt=='d')
			return dupFeatures;
		else
			return nondupFeatures;
		
	}
	
	public ArrayList<Double> getScores(char opt){
		if(opt=='d')
			return dup_scores;
		else
			return nondup_scores;
		
	}
	public ArrayList<Integer> getIndex1(char opt){
		if(opt=='d')
			return dup1;
		else
			return nondup1;
	}
	
	public ArrayList<Integer> getIndex2(char opt){
		if(opt=='d')
			return dup2;
		else
			return nondup2;
	}
	
	public static ArrayList<Integer> getFeatureWeights(String tuple1, String tuple2){
		int[] weight=null;
		String[] tokens1=tuple1.split(",");
		String[] tokens2=tuple2.split(",");
		
		ArrayList<Integer> result=new ArrayList<Integer>();
		for(int code=1;code<=25;code++){
		if(code==1)
			weight=BK.ExactMatch(tokens1, tokens2);
		else if(code==2)
			weight=BK.CommonToken(tokens1, tokens2);
		else if(code==3)
			weight=BK.CommonInteger(tokens1, tokens2);
		else if(code==4)
			weight=BK.CommonOrOffByOneInteger(tokens1, tokens2);
		else if(code==5)
			weight=BK.CommonNFirst(tokens1, tokens2,3);
		else if(code==6)
			weight=BK.CommonNFirst(tokens1, tokens2,5);
		else if(code==7)
			weight=BK.CommonNFirst(tokens1, tokens2,7);
		else if(code==8)
			weight=BK.CommonTokenNGram(tokens1, tokens2,2);
		else if(code==9)
			weight=BK.CommonTokenNGram(tokens1, tokens2,4);
		else if(code==10)
			weight=BK.CommonTokenNGram(tokens1, tokens2,6);
		else if(code>10&&code<=18){
			int p=code-11;
			String[] vals={"000","001","010","011","100","101","110","111"};
			String val=vals[p];
			boolean reverse= val.charAt(0)=='0' ? false : true;
			boolean mod= val.charAt(1)=='0' ? false : true;
			boolean four= val.charAt(2)=='0' ? false : true;
			weight=BK.soundex(tokens1, tokens2, reverse, mod, four);
			
		}
		else if(code>18&&code<=25){
			weight=new int[tokens1.length];
		}
		
			
		concatenate(result,weight);
		}
		return result;
	}
	
	private void checkFeatureLength(){
		for(int i=0; i<dupFeatures.size(); i++)
			if(dupFeatures.get(i).size()!=num_feats)
				System.out.println("ANOMALY LENGTH : " +dupFeatures.get(i).size());
		
		for(int i=0; i<nondupFeatures.size(); i++)
			if(nondupFeatures.get(i).size()!=num_feats)
				System.out.println("ANOMALY LENGTH : " +nondupFeatures.get(i).size());

	
	}
	
	//if opt is 0, then dup, otherwise nondup. Returns the entire concatenated ArrayList
		private ArrayList<Integer> getFeatureWeights_opt(int index, int opt){
			int[] weight=null;
			String[] tokens1=null;
			String[] tokens2=null;
			int index1=-1;
			int index2=-1;
			if(opt==0){
				index1=dup1.get(index);
				index2=dup2.get(index);		
			 tokens1=db.getTuples().get(index1).split(",");
			tokens2=db.getTuples().get(index2).split(",");
			}
			else{
				index1=nondup1.get(index);
				index2=nondup2.get(index);	
				 tokens1=db.getTuples().get(index1).split(",");
				 tokens2=db.getTuples().get(index2).split(",");
				
			}
			ArrayList<Integer> result=new ArrayList<Integer>();
			for(int code=1;code<=25;code++){
			if(code==1)
				weight=BK.ExactMatch(tokens1, tokens2);
			else if(code==2)
				weight=BK.CommonToken(tokens1, tokens2);
			else if(code==3)
				weight=BK.CommonInteger(tokens1, tokens2);
			else if(code==4)
				weight=BK.CommonOrOffByOneInteger(tokens1, tokens2);
			else if(code==5)
				weight=BK.CommonNFirst(tokens1, tokens2,3);
			else if(code==6)
				weight=BK.CommonNFirst(tokens1, tokens2,5);
			else if(code==7)
				weight=BK.CommonNFirst(tokens1, tokens2,7);
			else if(code==8)
				weight=BK.CommonTokenNGram(tokens1, tokens2,2);
			else if(code==9)
				weight=BK.CommonTokenNGram(tokens1, tokens2,4);
			else if(code==10)
				weight=BK.CommonTokenNGram(tokens1, tokens2,6);
			else if(code>10&&code<=18){
				int p=code-11;
				String[] vals={"000","001","010","011","100","101","110","111"};
				String val=vals[p];
				boolean reverse= val.charAt(0)=='0' ? false : true;
				boolean mod= val.charAt(1)=='0' ? false : true;
				boolean four= val.charAt(2)=='0' ? false : true;
				weight=BK.soundex(tokens1, tokens2, reverse, mod, four);
				
			}
			else if(code>18&&code<=25){
				weight=new int[tokens1.length];
			}
			
				
			concatenate(result,weight);
			}
			return result;
		}
		
		private static void concatenate(ArrayList<Integer> d,int[] weight){
			for(int i=0; i<weight.length; i++)
				d.add(weight[i]);
				
		}

}
