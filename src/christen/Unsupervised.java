package christen;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import java.util.*;




public class Unsupervised {
	
	private int[] atts;	//enumerates the attributes on which to block
	public int numatts;	//number of attributes
	public TFIDF db;	//database
	private Blocking b;	//blocking object
	
	int num_duplicates;	//number of overall best duplicates demanded by user
	int num_nonduplicates;
	
	private ArrayList<Double> bestpair_scores;
	private HashMap<Double,PairArrayList> bestpairs;
	int num_bestpairs=0;
	
	private ArrayList<Double> worstpair_scores;
	private HashMap<Double,PairArrayList> worstpairs;
	int num_worstpairs=0;	//actuals
	
	ArrayList<Double> dup_scores;
	ArrayList<Double> nondup_scores;
	
	 ArrayList<Integer> dup1;
	ArrayList<Integer> dup2;
	
	ArrayList<Integer> nondup1;
	ArrayList<Integer> nondup2;
	
	private ArrayList<UnsupervisedSingleAttribute> population=new ArrayList<UnsupervisedSingleAttribute>();
	
	public Unsupervised(String path) throws IOException{
		db=new TFIDF(path);
		int att=db.numatts;
		numatts=att;
		b=new Blocking(db);
		atts=new int[att];
		for(int i=0; i<att; i++)
			atts[i]=i;
		
		bestpair_scores=new ArrayList<Double>();
		bestpairs=new HashMap<Double,PairArrayList>();
		worstpair_scores=new ArrayList<Double>();
		worstpairs=new HashMap<Double,PairArrayList>();
		dup1=new ArrayList<Integer>();
		dup2=new ArrayList<Integer>();
		nondup1=new ArrayList<Integer>();
		nondup2=new ArrayList<Integer>();
		dup_scores=new ArrayList<Double>();
		nondup_scores=new ArrayList<Double>();
		
	}
	
	public Unsupervised(ArrayList<String> tuples, int num_atts) throws IOException{
		db=new TFIDF(tuples, num_atts);
		numatts=db.numatts;
		
		b=new Blocking(db);
		atts=new int[numatts];
		for(int i=0; i<numatts; i++)
			atts[i]=i;
		
		bestpair_scores=new ArrayList<Double>();
		bestpairs=new HashMap<Double,PairArrayList>();
		worstpair_scores=new ArrayList<Double>();
		worstpairs=new HashMap<Double,PairArrayList>();
		dup1=new ArrayList<Integer>();
		dup2=new ArrayList<Integer>();
		nondup1=new ArrayList<Integer>();
		nondup2=new ArrayList<Integer>();
		dup_scores=new ArrayList<Double>();
		nondup_scores=new ArrayList<Double>();
	}
	
	//ACCESS POINT after constructor: if don't want to print to file, put null as the last argument.
		//threshold1 is upper threshold
		public void computeDuplicatesCensus( int numduplicates, int numnonduplicates, double threshold1, double threshold2, String outfile) throws IOException{
			
			setNum(numduplicates,numnonduplicates);
			b.populateSimpleBKs();
			for(int i=0; i<atts.length; i++){
				//System.out.println("Run "+i);
				UnsupervisedSingleAttribute a=new UnsupervisedSingleAttribute(b,num_duplicates,num_nonduplicates);
				a.setRecordLinkageIDs("a","b",0);
				population.add(a);
				
				a.calc_scoresCensus(b.bkv_list.get(i),threshold1,threshold2);
				a.att=atts[i];
				
			}
			
			if(outfile!=null)
				printruntofile(outfile);
			else
				setUpDuplicates();
			/*
			if(b.cblocks!=null){
				for(int j:b.cblocks.keySet())
					System.out.println("Column "+j+" has "+b.cblocks.get(j).size()+" blocks with more than "+Blocking.c+" tuples");
					
			}*/
			
			//printDuplicates(5);
			
		}
		
		//ACCESS POINT after constructor: if don't want to print to file, put null as the last argument.
	//threshold1 is upper threshold
	public void computeDuplicates_optim( int numduplicates, int numnonduplicates, double threshold1, double threshold2, String outfile) throws IOException{
		
		setNum(numduplicates,numnonduplicates);
		b.populateSimpleBKs();
		for(int i=0; i<atts.length; i++){
			//System.out.println("Run "+i);
			UnsupervisedSingleAttribute a=new UnsupervisedSingleAttribute(b,num_duplicates,num_nonduplicates);
			population.add(a);
			
			a.calc_scores(b.bkv_list.get(i),threshold1,threshold2);
			a.att=atts[i];
			
		}
		
		if(outfile!=null)
			printruntofile(outfile);
		else
			setUpDuplicates();
		
		
		
	}
	
	
	private void incrementScoreArray(char opt, int count, double score){
		ArrayList<Double> scores=null;
		if(opt=='b')
			scores=dup_scores;
		else
			scores=nondup_scores;
		
		for(int i=0; i<count; i++)
			scores.add(score);
	}
	
	
	
	private void setNum(int numDuplicates, int numNonDuplicates){
		num_duplicates=numDuplicates;
		num_nonduplicates=numNonDuplicates;
	}

	private void printruntofile( String file) throws IOException{
		PrintWriter kk=new PrintWriter(new File(file)); 
		
		UnionPopulation();
		calcNumPairs();
		Collections.sort(bestpair_scores);
		
		int index=0;
		if(!(num_bestpairs<=num_duplicates))
		 index=calcDupIndex();	//calculate smallest index in bestpair_scores so that num_duplicates are covered
		 //forbidden=new HashSet<Double>();
		kk.println("BEST "+num_bestpairs+" PAIRS FOUND");
		for(int i=bestpair_scores.size()-1; i>=index; i--){
			
			kk.println("Score  : "+bestpair_scores.get(i));
		
			bestpairs.get(bestpair_scores.get(i)).printTuples(kk,db);
			
		}
		
		Collections.sort(worstpair_scores);
		 index=0;
		 if(!(num_worstpairs<=num_nonduplicates))
			 index=calcNonDupIndex();
		kk.println("BEST "+num_worstpairs+" NON DUPLICATE PAIRS FOUND");
		int count=0;
		for(int i=worstpair_scores.size()-1; i>=index; i--){
			
			kk.println("Score  : "+worstpair_scores.get(i));
			count+=worstpairs.get(worstpair_scores.get(i)).dup1.size();
			worstpairs.get(worstpair_scores.get(i)).printTuples(kk,db);
			
		}
		System.out.println(count);
		
		kk.close();
	}
	
	private void setUpDuplicates() throws IOException{
		
		UnionPopulation();
		calcNumPairs();
		Collections.sort(bestpair_scores);
		
		int index=0;
		if(!(num_bestpairs<=num_duplicates))
		 index=calcDupIndex();	//calculate smallest index in bestpair_scores so that num_duplicates are covered
		 //forbidden=new HashSet<Double>();
		dup1=new ArrayList<Integer>();
		dup2=new ArrayList<Integer>();
		int pcount=0;
		for(int i=bestpair_scores.size()-1; i>=index; i--){
			
			
			pcount+=bestpairs.get(bestpair_scores.get(i)).dup1.size();
			incrementScoreArray('b',bestpairs.get(bestpair_scores.get(i)).dup1.size(),bestpair_scores.get(i));
			concatenateDup(bestpairs.get(bestpair_scores.get(i)));
			
		}
		//System.out.println("Best pairs found: "+pcount);
		nondup1=new ArrayList<Integer>();
		nondup2=new ArrayList<Integer>();
		Collections.sort(worstpair_scores);
		 index=0;
		 if(!(num_worstpairs<=num_nonduplicates))
			 index=calcNonDupIndex();
		
		int count=0;
		for(int i=worstpair_scores.size()-1; i>=index; i--){
			
			
			count+=worstpairs.get(worstpair_scores.get(i)).dup1.size();
			incrementScoreArray('w',worstpairs.get(worstpair_scores.get(i)).dup1.size(),worstpair_scores.get(i));
			concatenateNonDup(worstpairs.get(worstpair_scores.get(i)));
			
		}
		//System.out.println("Worst pairs found: "+count);
		
		
	}
	
	private void concatenateDup(PairArrayList d){
		for(int i=0; i<d.dup1.size(); i++){
			dup1.add(d.dup1.get(i));
			dup2.add(d.dup2.get(i));
		}
		
	}
	private void concatenateNonDup(PairArrayList d){
		for(int i=0; i<d.dup1.size(); i++){
			nondup1.add(d.dup1.get(i));
			nondup2.add(d.dup2.get(i));
		}
	}
	
	private void UnionPopulation(){
		if(population.size()==0)
			return;
		else{
			bestpair_scores=new ArrayList<Double>(population.get(0).bestpair_scores);
			bestpairs=new HashMap<Double,PairArrayList>(population.get(0).bestpairs);
			worstpair_scores=new ArrayList<Double>(population.get(0).worstpair_scores);
			worstpairs=new HashMap<Double,PairArrayList>(population.get(0).worstpairs);
		}
		
		for(int i=1; i<population.size(); i++){
			ArrayList<Double> m=population.get(i).bestpair_scores;
			for(int j=0; j<m.size(); j++)
				if(bestpairs.containsKey(m.get(j))){
					PairArrayList q=bestpairs.get(m.get(j));
					q.combine(population.get(i).bestpairs.get(m.get(j)));
					bestpairs.remove(m.get(j));
					bestpairs.put(m.get(j),q);
				}
				else{
					bestpair_scores.add(m.get(j));
					bestpairs.put(m.get(j),population.get(i).bestpairs.get(m.get(j)));
				}
		}
		
		for(int i=1; i<population.size(); i++){
			ArrayList<Double> m=population.get(i).worstpair_scores;
			for(int j=0; j<m.size(); j++)
				if(worstpairs.containsKey(m.get(j))){
					PairArrayList q=worstpairs.get(m.get(j));
					q.combine(population.get(i).worstpairs.get(m.get(j)));
					worstpairs.remove(m.get(j));
					worstpairs.put(m.get(j),q);
				}
				else{
					worstpair_scores.add(m.get(j));
					worstpairs.put(m.get(j),population.get(i).worstpairs.get(m.get(j)));
				}
		}
			
		
	}
	
	private void calcNumPairs(){
		for(double p:bestpairs.keySet())
			num_bestpairs+=(bestpairs.get(p).dup1.size());
		
		for(double p:worstpairs.keySet())
			num_worstpairs+=(worstpairs.get(p).dup1.size());
		
	}
	private int calcDupIndex(){
		int sum=0;
		for(int i=bestpair_scores.size()-1; i>=0; i--){
			
			sum+=bestpairs.get(bestpair_scores.get(i)).dup1.size();
			if(sum>=num_duplicates){
				num_bestpairs=sum;
				return i;
				
			}
		}
		return 0;
	}
	
	private int calcNonDupIndex(){
		int sum=0;
		for(int i=worstpair_scores.size()-1; i>=0; i--){
			
			sum+=worstpairs.get(worstpair_scores.get(i)).dup1.size();
			if(sum>=num_nonduplicates){
				num_worstpairs=sum;
				return i;
				
			}
		}
		return 0;
	}
	
	
	
	
	
	
}
