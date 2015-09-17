package christen;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Scanner;

public class FeatureGen {
	
	
	
	ArrayList<Double> dup_scores;
	ArrayList<Double> nondup_scores;
	//dup1, dup2, nondup2, nondup2 are wrt to db.
	ArrayList<Integer> dup1;
	ArrayList<Integer> dup2;
	
	ArrayList<Integer> nondup1;
	ArrayList<Integer> nondup2;
	
	private ArrayList<ArrayList<Integer>> dupFeatures;
	private ArrayList<ArrayList<Integer>> nondupFeatures;
	
	private int num_feats=-1; //total number of features
	int num_attr=-1;
	
	TFIDF db;
	
	ArrayList<String> trainset;	//the training tuples
	ArrayList<Integer> mapping; //The ith element maps to the ith element of trainset
	HashMap<Integer,Integer> inv_mapping; //the key is the index to the actual set, while
	//the value is the index in trainset
	
	ArrayList<TFIDF> trainset_fields;
	ArrayList<TFIDF> trainset_tokens3;
	ArrayList<TFIDF> trainset_tokens5;
	
	
	
	//the infile is generated internally by the overall program. Must not be supplied by user
	public FeatureGen(String infile)throws IOException{
		Scanner in=new Scanner(new File(infile));
		String[] dup1=null;
		String[] dup2=null;
		String[] nondup1=null;
		String[] nondup2=null;
		ArrayList<String[]> dupfeats=new ArrayList<String[]>();
		ArrayList<String[]> nondupfeats=new ArrayList<String[]>();
		if(in.hasNextLine())
			 dup1=in.nextLine().split(", ");
		if(in.hasNextLine())
			 dup2=in.nextLine().split(", ");
		if(in.hasNextLine())
			 nondup1=in.nextLine().split(", ");
		if(in.hasNextLine())
			 nondup2=in.nextLine().split(", ");
			
		
		while(in.hasNextLine()){
			String k=in.nextLine();
			if(k.equals("*"))
				break;
			else
				dupfeats.add(k.split(", "));
		}
		while(in.hasNextLine())
			nondupfeats.add(in.nextLine().split(", "));
		
		in.close();
		populateStructures(dup1,dup2,nondup1,nondup2,dupfeats,nondupfeats);
		
		
		
	}
	
	
	public FeatureGen(Unsupervised pop, int numfeats, char dataset) throws IOException {
		trainset=new ArrayList<String>();
		mapping=new ArrayList<Integer>();
		trainset_fields=new ArrayList<TFIDF>();
		trainset_tokens3=new ArrayList<TFIDF>();
		trainset_tokens5=new ArrayList<TFIDF>();
		inv_mapping=new HashMap<Integer,Integer>();
		db=pop.db;
		setNum_feats(numfeats);
		if(dataset=='s')
			setUpCensusTraining(pop);
		else
			setUpTraining(pop);

	}
	
	public FeatureGen(Supervised sv, int numfeats, int numatts, String records)throws IOException{
		setNum_feats(numfeats);
		num_attr=numatts;
		dup1=sv.split1_traindup1;
		dup2=sv.split1_traindup2;
		nondup1=sv.split1_trainnondup1;
		nondup2=sv.split1_trainnondup2;
		db=new TFIDF(records);
		trainset=new ArrayList<String>();
		mapping=new ArrayList<Integer>();
		trainset_fields=new ArrayList<TFIDF>();
		trainset_tokens3=new ArrayList<TFIDF>();
		trainset_tokens5=new ArrayList<TFIDF>();
		inv_mapping=new HashMap<Integer,Integer>();
		setUpTraining();
	}
	
	public void setUpTraining(){
		System.out.println("ready to construct training set");
		makeTrainSet();
		constructTFIDFs();
		System.out.println("train set constructed");
	}
	
	public void setUpCensusTraining(Unsupervised a) throws IOException{
		num_attr=a.numatts;
		a.computeDuplicatesCensus(Parameters.dupDemanded,Parameters.nondupDemanded,Parameters.ut,Parameters.lt,null);
		dup_scores=a.dup_scores;
		nondup_scores=a.nondup_scores;
		dup1=a.dup1;
		dup2=a.dup2;
		nondup1=a.nondup1;
		nondup2=a.nondup2;
		a=null;
		
		System.out.println("ready to construct training set");
		makeTrainSet();
		constructTFIDFs();
		System.out.println("train set constructed");
		
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
		
		System.out.println("ready to construct training set");
		makeTrainSet();
		constructTFIDFs();
		System.out.println("train set constructed");
		
	}
	
	public void setUpFeatures(){
		setDupFeatures(new ArrayList<ArrayList<Integer>>(dup1.size()));
		setNondupFeatures(new ArrayList<ArrayList<Integer>>(nondup1.size()));
		
		System.out.println("constructing duplicate features");
		for(int i=0; i<dup1.size(); i++)
			getDupFeatures().add(getFeatureWeights_opt(i,0));
		
		System.out.println("constructing non duplicate features");
		for(int i=0; i<nondup1.size(); i++)
			getNondupFeatures().add(getFeatureWeights_opt(i,1));
		
		checkFeatureLength();
		
	}
	
	//will print lines in order: dup1, dup2, nondup2, nondup2, dupFeatures,*,nondupFeatures
	
	public void printToFile(String outfile)throws IOException{
		PrintWriter out=new PrintWriter(new File(outfile));
		out.println(dup1);
		out.println(dup2);
		out.println(nondup1);
		out.println(nondup2);
		for(int i=0; i<getDupFeatures().size(); i++)
			out.println(getDupFeatures().get(i));
		out.println("*");
		for(int i=0; i<getNondupFeatures().size(); i++)
			out.println(getNondupFeatures().get(i));
		out.close();
	}
	
	public ArrayList<ArrayList<Integer>> getFeatures(char opt){
		if(opt=='d')
			return getDupFeatures();
		else
			return getNondupFeatures();
		
	}
	
	public ArrayList<Double> getScores(char opt){
		if(opt=='d')
			return dup_scores;
		else
			return nondup_scores;
		
	}
	
	public void testFileReading()throws IOException{
		printToFile("C:\\Users\\Mayank\\Documents\\datasets\\tmp.txt");
		FeatureGen tmp=new FeatureGen("C:\\Users\\Mayank\\Documents\\datasets\\tmp.txt");
		if(tmp.getNum_feats()!=getNum_feats())
			System.out.println("num_feats unequal :"+tmp.getNum_feats());
		
		if(!isArrayListsEqual(tmp.dup1,dup1))
			System.out.println("dup1 failure");
		if(!isArrayListsEqual(tmp.dup2,dup2))
			System.out.println("dup2 failure");
		if(!isArrayListsEqual(tmp.nondup1,nondup1))
			System.out.println("nondup1 failure");
		if(!isArrayListsEqual(tmp.nondup2,nondup2))
			System.out.println("nondup2 failure");
		
		for(int i=0; i<getDupFeatures().size(); i++)
			if(!isArrayListsEqual(tmp.getDupFeatures().get(i),getDupFeatures().get(i)))
				System.out.println("dupFeatures failure");
		
		for(int i=0; i<getNondupFeatures().size(); i++)
			if(!isArrayListsEqual(tmp.getNondupFeatures().get(i),getNondupFeatures().get(i)))
				System.out.println("nondupFeatures failure");	
		
		if(getDupFeatures().size()!=tmp.getDupFeatures().size())
			System.out.println("dupFeatures length failure");
		
		if(getNondupFeatures().size()!=tmp.getNondupFeatures().size())
			System.out.println("nondupFeatures length failure");
		
		System.out.println("All tests complete");
				
	}
	//if both are null will return false
	private boolean isArrayListsEqual(ArrayList<Integer> a, ArrayList<Integer> b){
		if(a==null||b==null)
			return false;
		else if(a.size()!=b.size())
			return false;
		
		for(int i=0; i<a.size(); i++){
			int d=a.get(i);
			int e=b.get(i);
			if(d!=e){
				System.out.println("NOT EQUAL: "+a.get(i)+"  "+b.get(i));
				return false;
			}
		}
				
		
		return true;
		
	}

	private void makeTrainSet(){
		ArrayList<String> tuples=db.getTuples();
		
		for(int i=0; i<dup1.size(); i++){
			
				mapping.add(dup1.get(i));
				trainset.add(tuples.get(dup1.get(i)));
			
				mapping.add(dup2.get(i));
				trainset.add(tuples.get(dup2.get(i)));
			
		}
		
		for(int i=0; i<nondup1.size(); i++){
			
				mapping.add(nondup1.get(i));
				trainset.add(tuples.get(nondup1.get(i)));
			
				mapping.add(nondup2.get(i));
				trainset.add(tuples.get(nondup2.get(i)));
			
		}
		
		for(int i=0; i<mapping.size(); i++)
			inv_mapping.put(mapping.get(i), i);
			
			
	}
	
	private void constructTFIDFs(){
		//heuristically determine number of fields
		int fields=0;
		if(num_attr==-1){
			Random random=new Random(23432);
			int m=random.nextInt(trainset.size()-1);
			fields=trainset.get(m).split(",").length;
		}
		else
			fields=num_attr;
		
		ArrayList<ArrayList<String>> fieldtuples=new ArrayList<ArrayList<String>>(fields);
		ArrayList<ArrayList<String>> token3tuples=new ArrayList<ArrayList<String>>(fields);
		ArrayList<ArrayList<String>> token5tuples=new ArrayList<ArrayList<String>>(fields);
		
		for(int i=0; i<fields; i++){
			fieldtuples.add(new ArrayList<String>());
			token3tuples.add(new ArrayList<String>());
			token5tuples.add(new ArrayList<String>());
		}
		
		for(int i=0; i<trainset.size(); i++){
			String[] b=trainset.get(i).split(",");
			if(b.length!=fields)
				System.out.println("ERROR! FIELD SIZE UNEQUAL: TUPLE "+trainset.get(i)+" FIELDS "+fields);
			else
			{
				for(int j=0; j<fields; j++){
					fieldtuples.get(j).add(b[j]);
					token3tuples.get(j).add(constructNgrams(b[j],3));
					token5tuples.get(j).add(constructNgrams(b[j],5));
				}
				
			}
			
		}
		
		for(int i=0; i<fields; i++){
			try {
				trainset_fields.add(new TFIDF(fieldtuples.get(i)));
			
			trainset_tokens3.add(new TFIDF(token3tuples.get(i)));
			trainset_tokens5.add(new TFIDF(token5tuples.get(i)));} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		for(int i=0; i<fields; i++){
			trainset_fields.get(i).calculateweights();
			trainset_tokens3.get(i).calculateweights();
			trainset_tokens5.get(i).calculateweights();
		}
		
	}
	
	private void checkFeatureLength(){
		for(int i=0; i<getDupFeatures().size(); i++)
			if(getDupFeatures().get(i).size()!=getNum_feats())
				System.out.println("ANOMALY LENGTH : " +getDupFeatures().get(i).size());
		
		for(int i=0; i<getNondupFeatures().size(); i++)
			if(getNondupFeatures().get(i).size()!=getNum_feats())
				System.out.println("ANOMALY LENGTH : " +getNondupFeatures().get(i).size());

	
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
			else if(code==11||code==12||code==13||code==14||code==15){
				double delta=0.2;
				delta=delta*((code-11)+1);
				ArrayList<HashMap<String,Double>> p1=new ArrayList<HashMap<String,Double>>();
				ArrayList<HashMap<String,Double>> p2=new ArrayList<HashMap<String,Double>>();
				for(int i=0; i<trainset_fields.size();i++){
					p1.add(trainset_fields.get(i).weights.get(inv_mapping.get(index1)));
					p2.add(trainset_fields.get(i).weights.get(inv_mapping.get(index2)));
				}
				
				weight=BK.tokenTFIDF(p1, p2, delta);
			}
			else if(code==16||code==17||code==18||code==19||code==20){
				double delta=0.2;
				delta=delta*((code-16)+1);
				ArrayList<HashMap<String,Double>> p1=new ArrayList<HashMap<String,Double>>();
				ArrayList<HashMap<String,Double>> p2=new ArrayList<HashMap<String,Double>>();
				for(int i=0; i<trainset_tokens3.size();i++){
					p1.add(trainset_tokens3.get(i).weights.get(inv_mapping.get(index1)));
					p2.add(trainset_tokens3.get(i).weights.get(inv_mapping.get(index2)));
				}
				
				weight=BK.tokenTFIDF(p1, p2, delta);
			}
			else if(code==21||code==22||code==23||code==24||code==25){
				double delta=0.2;
				delta=delta*((code-21)+1);
				ArrayList<HashMap<String,Double>> p1=new ArrayList<HashMap<String,Double>>();
				ArrayList<HashMap<String,Double>> p2=new ArrayList<HashMap<String,Double>>();
				for(int i=0; i<trainset_tokens5.size();i++){
					p1.add(trainset_tokens5.get(i).weights.get(inv_mapping.get(index1)));
					p2.add(trainset_tokens5.get(i).weights.get(inv_mapping.get(index2)));
				}
				
				weight=BK.tokenTFIDF(p1, p2, delta);
			}
				
			concatenate(result,weight);
			}
			return result;
		}
		
		private void concatenate(ArrayList<Integer> d,int[] weight){
			for(int i=0; i<weight.length; i++)
				d.add(weight[i]);
				
		}
		
	//if opt is 0, then dup, otherwise nondup
	@SuppressWarnings("unused")
	private int[] getFeatureWeights(int code, int index, int opt){
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
		else if(code==11||code==12||code==13||code==14||code==15){
			double delta=0.2;
			delta=delta*((code-11)+1);
			ArrayList<HashMap<String,Double>> p1=new ArrayList<HashMap<String,Double>>();
			ArrayList<HashMap<String,Double>> p2=new ArrayList<HashMap<String,Double>>();
			for(int i=0; i<trainset_fields.size();i++){
				p1.add(trainset_fields.get(i).weights.get(inv_mapping.get(index1)));
				p2.add(trainset_fields.get(i).weights.get(inv_mapping.get(index2)));
			}
			
			weight=BK.tokenTFIDF(p1, p2, delta);
		}
		else if(code==16||code==17||code==18||code==19||code==20){
			double delta=0.2;
			delta=delta*((code-16)+1);
			ArrayList<HashMap<String,Double>> p1=new ArrayList<HashMap<String,Double>>();
			ArrayList<HashMap<String,Double>> p2=new ArrayList<HashMap<String,Double>>();
			for(int i=0; i<trainset_tokens3.size();i++){
				p1.add(trainset_tokens3.get(i).weights.get(inv_mapping.get(index1)));
				p2.add(trainset_tokens3.get(i).weights.get(inv_mapping.get(index2)));
			}
			
			weight=BK.tokenTFIDF(p1, p2, delta);
		}
		else if(code==21||code==22||code==23||code==24||code==25){
			double delta=0.2;
			delta=delta*((code-21)+1);
			ArrayList<HashMap<String,Double>> p1=new ArrayList<HashMap<String,Double>>();
			ArrayList<HashMap<String,Double>> p2=new ArrayList<HashMap<String,Double>>();
			for(int i=0; i<trainset_tokens5.size();i++){
				p1.add(trainset_tokens5.get(i).weights.get(inv_mapping.get(index1)));
				p2.add(trainset_tokens5.get(i).weights.get(inv_mapping.get(index2)));
			}
			
			weight=BK.tokenTFIDF(p1, p2, delta);
		}
			
		return weight;
	}
	
	private String constructNgrams(String a, int n){
		if(n<1)
			return null;
		HashSet<String> p=new HashSet<String>();
		String[] q=a.split(Parameters.splitstring);
		for(int i=0; i<q.length; i++)
			for(int j=0; j<=q[i].length()-n; j++)
				p.add(q[i].substring(j,j+n));
		ArrayList<String> s=new ArrayList<String>(p);
		Collections.sort(s);
		String result="";
		for(int i=0; i<s.size(); i++)
			result+=(s.get(i)+" ");
		
		if(result.length()>0)
			return result.substring(0, result.length()-1);
		else
			return result;
	}
	
	//will throw exceptions if any of these are not populated/in unexpected ways
	private void populateStructures(String[] d1, String[] d2, String[] nd1, String[] nd2, ArrayList<String[]> df, ArrayList<String[]> ndf){
		dup1=new ArrayList<Integer>();
		dup2=new ArrayList<Integer>();
		nondup1=new ArrayList<Integer>();
		nondup2=new ArrayList<Integer>();
		setDupFeatures(new ArrayList<ArrayList<Integer>>());
		setNondupFeatures(new ArrayList<ArrayList<Integer>>());
		
		convertToArrayList(d1,dup1);
		convertToArrayList(d2,dup2);
		convertToArrayList(nd1,nondup1);
		convertToArrayList(nd2,nondup2);
		
		
		
		for(int i=0; i<df.size(); i++){
			ArrayList<Integer> tmp=new ArrayList<Integer>();
			convertToArrayList(df.get(i),tmp);
			getDupFeatures().add(tmp);
		}
		
		for(int i=0; i<ndf.size(); i++){
			ArrayList<Integer> tmp=new ArrayList<Integer>();
			convertToArrayList(ndf.get(i),tmp);
			getNondupFeatures().add(tmp);
		}
		
		setNum_feats(df.get(0).length);
	}
	
	private void convertToArrayList(String[] p, ArrayList<Integer> q){
		p[0]=p[0].substring(1,p[0].length());
		p[p.length-1]=p[p.length-1].substring(0,p[p.length-1].length()-1);
		for(int i=0; i<p.length; i++)
			q.add(Integer.parseInt(p[i]));
	}


	public int getNum_feats() {
		return num_feats;
	}


	public void setNum_feats(int num_feats) {
		this.num_feats = num_feats;
	}


	public ArrayList<ArrayList<Integer>> getDupFeatures() {
		return dupFeatures;
	}


	public void setDupFeatures(ArrayList<ArrayList<Integer>> dupFeatures) {
		this.dupFeatures = dupFeatures;
	}


	public ArrayList<ArrayList<Integer>> getNondupFeatures() {
		return nondupFeatures;
	}


	public void setNondupFeatures(ArrayList<ArrayList<Integer>> nondupFeatures) {
		this.nondupFeatures = nondupFeatures;
	}

}
