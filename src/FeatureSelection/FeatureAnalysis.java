package FeatureSelection;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

public class FeatureAnalysis {

	/**
	 * @param args
	 */
	private Fisher a;
	private ArrayList<Double> scores; //deep sorted (Descending) clone of a.scores. When features are extended, this gets modified, not 'scores' in Fisher.
	private HashMap<Double,HashSet<Integer>> scoremap; //indexes into the scores in Fisher
	
	private ArrayList<HashSet<Integer>> conjuncts=new ArrayList<HashSet<Integer>>();
	HashMap<HashSet<Integer>,Double> conjunct_scores=new HashMap<HashSet<Integer>,Double>();
	HashMap<HashSet<Integer>,Integer> inv_conjunct=new HashMap<HashSet<Integer>,Integer>(); //references feature index
	int k=-1;
	private static double current_avg=0.0;
	private int feats_num=0;	//might not match with num_features in Fisher, but
	//will always match with the actual number of features
	
	public FeatureAnalysis(Fisher b){
		setA(b);
		
		HashSet<Double> scoreset=new HashSet<Double>();
		setScoremap(new HashMap<Double,HashSet<Integer>>());
		for(int i=0; i<b.scores.size(); i++){
			scoreset.add(b.scores.get(i));
			if(getScoremap().containsKey(b.scores.get(i))){
				getScoremap().get(b.scores.get(i)).add(i);
			}
			else{
				getScoremap().put(b.scores.get(i),new HashSet<Integer>());
				getScoremap().get(b.scores.get(i)).add(i);
			}
		}
		setScores(new ArrayList<Double>(scoreset));
		Collections.sort(getScores());
		ArrayList<Double> tmp=new ArrayList<Double>();
		for(int i=getScores().size()-1; i>=0; i--)
			tmp.add(getScores().get(i));
		setScores(tmp);
		setFeats_num(getA().get_num_features());
			
	}
	
	public static FeatureAnalysis clone(FeatureAnalysis a){
		Fisher m=Fisher.clone(a.getA());
		return new FeatureAnalysis(m);
	}
	
	public void recompute(Fisher b){
		setA(b);
		
		HashSet<Double> scoreset=new HashSet<Double>();
		setScoremap(new HashMap<Double,HashSet<Integer>>());
		for(int i=0; i<b.scores.size(); i++){
			scoreset.add(b.scores.get(i));
			if(getScoremap().containsKey(b.scores.get(i))){
				getScoremap().get(b.scores.get(i)).add(i);
			}
			else{
				getScoremap().put(b.scores.get(i),new HashSet<Integer>());
				getScoremap().get(b.scores.get(i)).add(i);
			}
		}
		setScores(new ArrayList<Double>(scoreset));
		Collections.sort(getScores());
		ArrayList<Double> tmp=new ArrayList<Double>();
		for(int i=getScores().size()-1; i>=0; i--)
			tmp.add(getScores().get(i));
		setScores(tmp);
		setFeats_num(getA().get_num_features());
	}
	/*
	 * Prints indices in original scores arraylist. Treat with caution when interpreting indices.
	 */
	public void printBestFeatures(){
		for(int i=0; i<getScores().size(); i++){
			System.out.println(getScores().get(i)+" "+new ArrayList<Integer>(getScoremap().get(getScores().get(i))));
			
		}
	}
	
	public void printBestFeatures(String file) throws FileNotFoundException{
		PrintWriter kk=new PrintWriter(new File(file));
		for(int i=0; i<getScores().size(); i++)
			kk.println(getScores().get(i)+" "+new ArrayList<Integer>(getScoremap().get(getScores().get(i))));
		kk.close();
	}
	
	public void composeDNF(){
		
		
		for(int i=0; i<getA().get_num_features(); i++){
			current_avg=computeAverageScores(getScores());
			HashSet<Integer> q=new HashSet<Integer>();
			q.add(i);
			HashSet<Integer> res=addConjuncts(q);
			while(res!=null)
				res=addConjuncts(res);
		}
		System.out.println("extending features now");
		System.out.println("conjuncts size "+getConjuncts().size());
		extendFeatures();
		
	}
	
	public void composeDNF(int k){
		this.k=k;
		
		
		for(int i=0; i<getA().get_num_features(); i++){
			current_avg=computeAverageScores(getScores());
			HashSet<Integer> q=new HashSet<Integer>();
			q.add(i);
			HashSet<Integer> res=addConjuncts(q);
			while(res!=null)
				res=addConjuncts(res);
		}
		System.out.println("extending features now");
		System.out.println("conjuncts size "+getConjuncts().size());
		extendFeatures();
		
	}
	
	
	private void extendFeatures(){
		for(int i=0; i<getA().get_classes().size(); i++){
			ArrayList<ArrayList<Integer>> cl=getA().features.get(getA().get_classes().get(i));
			for(int j=0; j<cl.size(); j++){
				ArrayList<Integer> sample=cl.get(j);
				for(int m=0; m<getConjuncts().size(); m++){
			
				
				
					ArrayList<Integer> d=new ArrayList<Integer>(getConjuncts().get(m));
					int sum=0;
					for(int k=0; k<d.size(); k++)
						sum+=sample.get(d.get(k));
					if(sum==d.size())
						sample.add(1);
					else
						sample.add(0);
					inv_conjunct.put(getConjuncts().get(m),sample.size()-1);
					double score=conjunct_scores.get(getConjuncts().get(m));
					if(!getScoremap().containsKey(score))
						getScoremap().put(score, new HashSet<Integer>());
					getScoremap().get(score).add(sample.size()-1);
					getScores().add(score);
					setFeats_num(getFeats_num() + 1);
					
				}
				
				
			}
		}
		Collections.sort(getScores());
		ArrayList<Double> tmp=new ArrayList<Double>();
		for(int i=getScores().size()-1; i>=0; i--)
			tmp.add(getScores().get(i));
		setScores(tmp);
	}
	
	//add all new conjuncts better than current_avg, return the best scoring one.
	private HashSet<Integer> addConjuncts(HashSet<Integer> current){
		if(current.size()==k)
			return null;
		double avg_result=0.0;
		HashSet<Integer> res=null;
		int count=0;
		double best=current_avg;
		for(int i=0; i<getA().get_num_features(); i++){
			double eval=0.0;
			if(current.contains(i))
				continue;
			eval=evaluateFisher(current,i);
			if(eval>=current_avg){
				HashSet<Integer> p=new HashSet<Integer>(current);
				p.add(i);
				if(getConjuncts().contains(p))
					continue;
				getConjuncts().add(p);
				conjunct_scores.put(p,eval);
				avg_result+=eval;
				count++;
				if(eval>=best){
					best=eval;
					res=new HashSet<Integer>(p);
				}
			}
				
		}
		if(count!=0)
			current_avg=avg_result/count;
		else
			current_avg=0;
		
		return res;
	}
	
	//evaluate Fisher score of all indexes in current ^ featIndex
	private double evaluateFisher(HashSet<Integer> current, int featIndex){
		ArrayList<Integer> d=new ArrayList<Integer>(current);
		d.add(featIndex);
		ArrayList<ArrayList<Integer>> vectors=new ArrayList<ArrayList<Integer>>();
		for(int i=0; i<getA().get_classes().size(); i++){
			ArrayList<ArrayList<Integer>> cl=getA().features.get(getA().get_classes().get(i));
			ArrayList<Integer> tmp=new ArrayList<Integer>();
			for(int j=0; j<cl.size(); j++){
				int sum=0;
				for(int k=0; k<d.size(); k++)
					sum+=cl.get(j).get(d.get(k));
				if(sum==d.size())
					tmp.add(1);
				else
					tmp.add(0);
				
			}
			vectors.add(tmp);
			
		}
		ArrayList<double[]> stats=new ArrayList<double[]>();
		int count=0;
		double sum=0.0;
		for(int i=0; i<vectors.size(); i++){
			double[] res=computeMeanVariance(vectors.get(i));
			sum+=(vectors.get(i).size()*res[0]);
			count+=vectors.get(i).size();
			stats.add(res);
		}
		sum=sum/count;
		
		double num=0.0;
		double denom=0.0;
		for(int i=0; i<stats.size(); i++){
			num+=(vectors.get(i).size()*Math.pow(stats.get(i)[0]-sum, 2));
			denom+=(vectors.get(i).size()*Math.pow(stats.get(i)[1], 2));
		}
		if(denom==0.0)
			return 0;
		else
			return num/denom;
	}
	
	private double[] computeMeanVariance(ArrayList<Integer> d){
		double[] res=new double[2];
		int sum=0;
		for(int i=0; i<d.size();i++)
			sum+=d.get(i);
		res[0]=sum*1.0/d.size();
		for(int i=0; i<d.size(); i++)
			res[1]+=(Math.pow(res[0]-d.get(i),2));
		res[1]/=d.size();
		return res;
	}
	
	private double computeAverageScores(ArrayList<Double> s){
		double result=0.0;
		if(s==null||s.size()==0)
			return result;
		for(int i=0; i<s.size(); i++)
			result+=s.get(i);
		
		return result/(s.size());
	}

	public Fisher getA() {
		return a;
	}

	public void setA(Fisher a) {
		this.a = a;
	}

	public ArrayList<Double> getScores() {
		return scores;
	}

	public void setScores(ArrayList<Double> scores) {
		this.scores = scores;
	}

	public HashMap<Double,HashSet<Integer>> getScoremap() {
		return scoremap;
	}

	public void setScoremap(HashMap<Double,HashSet<Integer>> scoremap) {
		this.scoremap = scoremap;
	}

	public int getFeats_num() {
		return feats_num;
	}

	public void setFeats_num(int feats_num) {
		this.feats_num = feats_num;
	}

	public ArrayList<HashSet<Integer>> getConjuncts() {
		return conjuncts;
	}

	public void setConjuncts(ArrayList<HashSet<Integer>> conjuncts) {
		this.conjuncts = conjuncts;
	}
	
	

}
