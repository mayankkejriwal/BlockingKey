package christen;

import java.util.*;


public class UnsupervisedSingleAttribute {
	
	
	
	Blocking a;
	
	
	ArrayList<Double> bestpair_scores;
	HashMap<Double,PairArrayList> bestpairs;
	int actualbest=0;
	
	ArrayList<Double> worstpair_scores;
	HashMap<Double,PairArrayList> worstpairs;
	int actualworst=0;	//the actual number of pairs found. At least as great as maxpairs
	
	public int att=-1; //records on which attribute we're scoring
	
	int maxpairs_dup;	//actual number of pairs could be higher since we
	//consider groups of pairs in score buckets
	int maxpairs_nondup;
	
	String id1=null;
	String id2=null;
	int id_att=-1;
	
	public UnsupervisedSingleAttribute(Blocking a, int dupPairs, int nonDupPairs){
		this.a=a;
		
		bestpair_scores=new ArrayList<Double>();
		bestpairs=new HashMap<Double,PairArrayList>();
		
		worstpair_scores=new ArrayList<Double>();
		worstpairs=new HashMap<Double,PairArrayList>();
		if(dupPairs!=-1)
			this.maxpairs_dup=dupPairs;
		else
			this.maxpairs_dup=Integer.MAX_VALUE;
		
		if(nonDupPairs!=-1)
			this.maxpairs_nondup=nonDupPairs;
		else
			this.maxpairs_nondup=Integer.MAX_VALUE;
	}
	
	public void reinitialize(){
		
		bestpair_scores=new ArrayList<Double>();
		bestpairs=new HashMap<Double,PairArrayList>();
		worstpair_scores=new ArrayList<Double>();
		worstpairs=new HashMap<Double,PairArrayList>();
		att=-1;
	}
	
	public void setRecordLinkageIDs(String i1, String i2, int att){
		id1=i1;
		id2=i2;
		id_att=att;
	}
	
	public void calc_scores(HashMap<String,ArrayList<Integer>> bkv, double threshold1, double threshold2){
	reinitialize();
	for(String m: bkv.keySet())
		for(int i=0; i<bkv.get(m).size(); i++){	
		
			int pair1_index=bkv.get(m).get(i);
			HashMap<String,Double> tmp1=a.db.weights.get(pair1_index);
			for(int j=i+1;j<bkv.get(m).size() && j-i<Parameters.c; j++){
				int pair2_index=bkv.get(m).get(j);
				
				double sum=0.0;
				
				
				
				
				HashMap<String,Double> tmp2=a.db.weights.get(pair2_index);
				for(String p:tmp1.keySet())
					if(tmp2.containsKey(p))
							sum+=(tmp1.get(p)*tmp2.get(p));
					
					
					
				
				
				
				if(sum>=threshold1){
					
					if(bestpairs.containsKey(sum)){
						
						bestpairs.get(sum).add(pair1_index, pair2_index);
					}
					else if(bestpair_scores.size()<maxpairs_dup){
						bestpair_scores.add(sum);
						Collections.sort(bestpair_scores);
						bestpairs.put(sum, new PairArrayList());
						bestpairs.get(sum).add(pair1_index, pair2_index);
					}
					else if(sum>(double)bestpair_scores.get(0)){
						double oldsum=bestpair_scores.get(0);
						bestpair_scores.set(0,sum);
						Collections.sort(bestpair_scores);
						bestpairs.remove(oldsum);
						bestpairs.put(sum,new PairArrayList());
						bestpairs.get(sum).add(pair1_index,pair2_index);
						
					}
				
				}
				else if(sum<=threshold2){
					if(worstpairs.containsKey(sum)){
						
						worstpairs.get(sum).add(pair1_index, pair2_index);
					}
					else if(worstpair_scores.size()<maxpairs_nondup){
						worstpair_scores.add(sum);
						Collections.sort(worstpair_scores);
						worstpairs.put(sum, new PairArrayList());
						worstpairs.get(sum).add(pair1_index, pair2_index);
					}
					else if(sum>(double)worstpair_scores.get(worstpair_scores.size()-1)){
						double oldsum=worstpair_scores.get(0);
						worstpair_scores.set(0,sum);
						Collections.sort(worstpair_scores);
						worstpairs.remove(oldsum);
						worstpairs.put(sum,new PairArrayList());
						worstpairs.get(sum).add(pair1_index,pair2_index);
						
					}
					
				}
			
					
			}//second for loop ends
			
			
					
						
	}
		
	  
		trimScores();
		
		
	}
	
	public void calc_scoresCensus(HashMap<String,ArrayList<Integer>> bkv, double threshold1, double threshold2){
		reinitialize();
		for(String m: bkv.keySet()){
			ArrayList<Integer> Alist=new ArrayList<Integer>();
			ArrayList<Integer> Blist=new ArrayList<Integer>();
			populateListsCensus(bkv.get(m),Alist,Blist);
			ArrayList<Integer> large=Blist;
			ArrayList<Integer> small=Alist;
			if(Alist.size()>Blist.size()){
				large=Alist;
				small=Blist;
			}
			for(int i=0; i<large.size(); i++){	
			
				int pair1_index=large.get(i);
				HashMap<String,Double> tmp1=a.db.weights.get(pair1_index);
				int lim=-1;
				if(small.size()<=Parameters.c)
					lim=small.size()-1;
				else
					lim=small.size()-Parameters.c;
				for(int jj=0; jj<small.size() && jj<=lim; jj++)
				for(int j=jj;j<small.size() && j-jj<Parameters.c; j++){
					int pair2_index=small.get(j);
					
					double sum=0.0;
					
					
					
					
					HashMap<String,Double> tmp2=a.db.weights.get(pair2_index);
					for(String p:tmp1.keySet())
						if(tmp2.containsKey(p))
								sum+=(tmp1.get(p)*tmp2.get(p));
						
						
						
					
					
					
					if(sum>=threshold1){
						
						if(bestpairs.containsKey(sum)){
							
							bestpairs.get(sum).add(pair1_index, pair2_index);
						}
						else if(bestpair_scores.size()<maxpairs_dup){
							bestpair_scores.add(sum);
							Collections.sort(bestpair_scores);
							bestpairs.put(sum, new PairArrayList());
							bestpairs.get(sum).add(pair1_index, pair2_index);
						}
						else if(sum>(double)bestpair_scores.get(0)){
							double oldsum=bestpair_scores.get(0);
							bestpair_scores.set(0,sum);
							Collections.sort(bestpair_scores);
							bestpairs.remove(oldsum);
							bestpairs.put(sum,new PairArrayList());
							bestpairs.get(sum).add(pair1_index,pair2_index);
							
						}
					
					}
					else if(sum<=threshold2){
						if(worstpairs.containsKey(sum)){
							
							worstpairs.get(sum).add(pair1_index, pair2_index);
						}
						else if(worstpair_scores.size()< maxpairs_nondup){
							worstpair_scores.add(sum);
							Collections.sort(worstpair_scores);
							worstpairs.put(sum, new PairArrayList());
							worstpairs.get(sum).add(pair1_index, pair2_index);
						}
						else if(sum>(double)worstpair_scores.get(worstpair_scores.size()-1)){
							double oldsum=worstpair_scores.get(0);
							worstpair_scores.set(0,sum);
							Collections.sort(worstpair_scores);
							worstpairs.remove(oldsum);
							worstpairs.put(sum,new PairArrayList());
							worstpairs.get(sum).add(pair1_index,pair2_index);
							
						}
						
					}
				
						
				}//second for loop ends
				
				
						
							
		}
			
		}
		  
			trimScores();
			
			
		}
	
	
	
	//this function will not initialize anything. Hardcoded for census
	private void populateListsCensus(ArrayList<Integer> block,ArrayList<Integer> Alist,ArrayList<Integer> Blist){
		for(int i=0; i<block.size(); i++){
			String k=a.db.getTuples().get(block.get(i)).split(",")[id_att];
			if(k.equals(id1))
				Alist.add(block.get(i));
			else if(k.equals(id2))
				Blist.add(block.get(i));
		}
	}

	//populates 'actual' variables, and trims score structures
	private void trimScores(){
		//first do for best pairs
		int count=0;
		int i=bestpair_scores.size()-1;
		if(i>=0){
		count=bestpairs.get(bestpair_scores.get(i)).dup1.size();
		i--;
		while(count<maxpairs_dup&&i>=0){
			count+=bestpairs.get(bestpair_scores.get(i)).dup1.size();
			i--;
		}
		
		for(int p=i; p>=0; p--){
			bestpairs.remove(bestpair_scores.get(p));
			bestpair_scores.remove(p);
		}
		actualbest=count;
		}
		else actualbest=0;
		//now do for worst pairs
		count=0;
		i=worstpair_scores.size()-1;
		if(i>=0){
		count=worstpairs.get(worstpair_scores.get(i)).dup1.size();
		i--;
		while(count<maxpairs_nondup&&i>=0){
			count+=worstpairs.get(worstpair_scores.get(i)).dup1.size();
			i--;
		}
		
		for(int p=i; p>=0; p--){
			worstpairs.remove(worstpair_scores.get(p));
			worstpair_scores.remove(p);
		}
		actualworst=count;
		}
		else
			actualworst=0;
	}
	
	
	
	
}
