package FeatureSelection;

import java.util.*;

public class Fisher {
	
	private ArrayList<Integer> classes; //positive or negative
	public HashMap<Integer,ArrayList<ArrayList<Integer>>> features; //use class label
	//to refer to the arraylist containing the features corresponding to that class. 
	private int orig_num_features;
	private int total_samples=0;
	
	private ArrayList<ArrayList<Double>> mean;	//use first arraylist to correspond to class
	private ArrayList<ArrayList<Double>> variance;
	
	private ArrayList<Double> overall_mean;	//correspond to features
	private ArrayList<Double> overall_variance;
	
	ArrayList<Double> scores;
	
	//return a deep copy clone
	public static Fisher clone(Fisher a){
		Fisher result=new Fisher(a.get_num_features(), a.get_total_samples());
		result.set_classes(cloneArrayListInt(a.get_classes()));
		result.features=new HashMap<Integer, ArrayList<ArrayList<Integer>>>();
		for(int i:a.features.keySet())
			result.features.put(i, cloneMultArrayListInt(a.features.get(i)));
		result.set_mean(cloneMultArrayListDouble(a.get_mean()));
		result.set_variance(cloneMultArrayListDouble(a.get_variance()));
		result.set_overall_mean(cloneArrayListDouble(a.get_overall_mean()));
		result.set_overall_variance(cloneArrayListDouble(a.get_overall_variance()));
		result.scores=cloneArrayListDouble(a.scores);
		
		
		return result;
	}
	
	private Fisher(int num_feats, int samples){
		set_num_features(num_feats);
		set_total_samples(samples);
	}
	
	//deep clone arraylists of various types
	private static ArrayList<Integer> cloneArrayListInt(ArrayList<Integer> s){
		ArrayList<Integer> result=new ArrayList<Integer>(s.size());
		for(int i=0; i<s.size(); i++)
			result.add((int) s.get(i));
		
		return result;
	}
	private static ArrayList<ArrayList<Integer>> cloneMultArrayListInt(ArrayList<ArrayList<Integer>> s){
		ArrayList<ArrayList<Integer>> result=new ArrayList<ArrayList<Integer>>(s.size());
		for(int i=0; i<s.size(); i++)
			result.add(cloneArrayListInt(s.get(i)));
		
		return result;
	}
	
	
	
	private static ArrayList<Double> cloneArrayListDouble(ArrayList<Double> s){
		ArrayList<Double> result=new ArrayList<Double>(s.size());
		for(int i=0; i<s.size(); i++)
			result.add((double) s.get(i));
		
		return result;
	}
	
	private static ArrayList<ArrayList<Double>> cloneMultArrayListDouble(ArrayList<ArrayList<Double>> s){
		ArrayList<ArrayList<Double>> result=new ArrayList<ArrayList<Double>>(s.size());
		for(int i=0; i<s.size(); i++)
			result.add(cloneArrayListDouble(s.get(i)));
		
		return result;
	}
	
	//users are not expected to provide class labels, just clusters of features
	//classes are labelled from 1 to the number of clusters. Statistics are not computed
	
	public Fisher(int num_feats,ArrayList<ArrayList<Integer>>...feature){
		set_classes(new ArrayList<Integer>(feature.length));
		features=new HashMap<Integer,ArrayList<ArrayList<Integer>>>();
		int num=1;
		for(int i=0; i<feature.length; i++){
			get_classes().add(num);
			features.put(num, feature[i]);
			num++;
			
		}
		scores=new ArrayList<Double>(num_feats);
		for(int i:features.keySet())
			set_total_samples(get_total_samples() + (features.get(i).size()));
		set_num_features(num_feats);
		set_overall_mean(new ArrayList<Double>());
		set_overall_variance(new ArrayList<Double>());
		set_mean(new ArrayList<ArrayList<Double>>());
		set_variance(new ArrayList<ArrayList<Double>>());
		
	}
	
	//only statistics are reinitialized
	public void reinitialize(){
		scores=new ArrayList<Double>();
		set_overall_mean(new ArrayList<Double>());
		set_overall_variance(new ArrayList<Double>());
		set_mean(new ArrayList<ArrayList<Double>>());
		set_variance(new ArrayList<ArrayList<Double>>());
	}
	
	public void computeStatistics(){
		ArrayList<ArrayList<Integer>> big=new ArrayList<ArrayList<Integer>>();
		for(int i=0; i<get_classes().size(); i++){
			UnionArrayList(big,features.get(get_classes().get(i)));
			get_mean().add(returnMean(features.get(get_classes().get(i))));
			get_variance().add(returnVariance(features.get(get_classes().get(i))));
		}
		set_overall_mean(returnMean(big));
		set_overall_variance(returnVariance(big));
		
		
				
		for(int i=0; i<get_num_features(); i++){
			double num=0.0;
			double denom=0.0;
			for(int j=0; j<get_classes().size(); j++){
				num+=(features.get(get_classes().get(j)).size()*Math.pow(get_mean().get(j).get(i)-get_overall_mean().get(i), 2));
				denom+=(features.get(get_classes().get(j)).size()*Math.pow(get_variance().get(j).get(i), 2));
			}
			if(denom!=0)
				scores.add(num/denom);
			else
				scores.add(0.0);
		}
		
			
		
	}
	
	private <T>void UnionArrayList(ArrayList<T> a, ArrayList<T> b){
		for(int i=0; i<b.size(); i++)
			a.add(b.get(i));
	}
	
	private void addarraylist(ArrayList<Double> a, ArrayList<Integer> b){
		
		
		for(int i=0; i<a.size(); i++)
			a.set(i,a.get(i)+b.get(i));
		
		
	}
	
	private void addarraylist2(ArrayList<Double> a, ArrayList<Double> b){
		
		
		for(int i=0; i<a.size(); i++)
			a.set(i,a.get(i)+b.get(i));
		
		
	}
	private ArrayList<Double> returnMean(ArrayList<ArrayList<Integer>> a){
		ArrayList<Double> result=new ArrayList<Double>();
		for(int i=0; i<get_num_features(); i++)
			result.add(0.0);
		for(int i=0; i<a.size(); i++)
			addarraylist(result,a.get(i));
		
		multarraylist(1.0/a.size(),result);
		
		return result;
		
	}
	
	private void multarraylist(double q, ArrayList<Double> p){
		for(int i=0; i<p.size(); i++)
			p.set(i,p.get(i)*q);
	}
	
	private ArrayList<Double> returnVariance(ArrayList<ArrayList<Integer>> a){
		ArrayList<Double> mean=returnMean(a);
		//System.out.println(mean.size());
		
		ArrayList<Double> result=new ArrayList<Double>();
		for(int i=0; i<get_num_features(); i++)
			result.add(0.0);
		
		for(int j=0; j<a.size(); j++)
		{
			ArrayList<Integer> d=a.get(j);
			
			ArrayList<Double> temp=new ArrayList<Double>();
			for(int i=0; i<d.size(); i++)
				temp.add(Math.pow(d.get(i)-mean.get(i),2));
			addarraylist2(result,temp);
		}
		
		multarraylist(1.0/a.size(),result);
		
		return result;
		
	}

	public ArrayList<Double> get_scores(){
		return scores;
	}

	public int get_num_features() {
		return orig_num_features;
	}

	public void set_num_features(int num_features) {
		this.orig_num_features = num_features;
	}

	public int get_total_samples() {
		return total_samples;
	}

	public void set_total_samples(int total_samples) {
		this.total_samples = total_samples;
	}

	public ArrayList<ArrayList<Double>> get_variance() {
		return variance;
	}

	public void set_variance(ArrayList<ArrayList<Double>> variance) {
		this.variance = variance;
	}

	public ArrayList<Integer> get_classes() {
		return classes;
	}

	public void set_classes(ArrayList<Integer> classes) {
		this.classes = classes;
	}

	public ArrayList<ArrayList<Double>> get_mean() {
		return mean;
	}

	public void set_mean(ArrayList<ArrayList<Double>> mean) {
		this.mean = mean;
	}

	public ArrayList<Double> get_overall_mean() {
		return overall_mean;
	}

	public void set_overall_mean(ArrayList<Double> overall_mean) {
		this.overall_mean = overall_mean;
	}

	public ArrayList<Double> get_overall_variance() {
		return overall_variance;
	}

	public void set_overall_variance(ArrayList<Double> overall_variance) {
		this.overall_variance = overall_variance;
	}
}
