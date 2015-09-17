package christen;



import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;



public class TFIDF {
	
	ArrayList<HashMap<String,Integer>> term_frequency;			//Term Frequency
	private ArrayList<String> tuples;
	HashMap<String,Integer> inverse_DF;	
	public ArrayList<HashMap<String,Double>> weights;
	boolean weightset=false;	//set to true once weights get calculated in local IDF def.
	public int corpussize=0;
	public int numatts=0;
	
	//will deal with quotes case but quotes must not be inside quotes
	//first line is schema spec. and will be discarded. However, it will be used
	//for determining numatts.
	public TFIDF(String file) throws IOException{
		Scanner in=new Scanner(new FileReader(new File(file)));
		ArrayList<String> tuples1=new ArrayList<String>();
		if(in.hasNextLine()){
			numatts=(new CSVParser()).parseLine(in.nextLine()).length;
			
		}
		while(in.hasNextLine()){
			String line=in.nextLine().toLowerCase();
			
			tuples1.add(line);
		}
		
		in.close();
		
		
		
		setTuples(tuples1);
		corpussize=getTuples().size();
		term_frequency = new ArrayList<HashMap<String,Integer>>(getTuples().size());
		inverse_DF = new HashMap<String,Integer>();
		weights = new ArrayList<HashMap<String,Double>>(getTuples().size()) ;
		calculate();
			
	}
	public TFIDF(ArrayList<String> tuples1) throws IOException
	{
		setTuples(tuples1);
		if(tuples1.size()!=0)
			numatts=(new CSVParser()).parseLine(tuples1.get(0)).length;
			
		corpussize=getTuples().size();
		term_frequency = new ArrayList<HashMap<String,Integer>>(getTuples().size());
		inverse_DF = new HashMap<String,Integer>();
		weights = new ArrayList<HashMap<String,Double>>(getTuples().size()) ;
		calculate();
	}
	
	public TFIDF(ArrayList<String> tuples, int atts)throws IOException{
		numatts=atts;
		ArrayList<String> tuples1=tuples;
		
		
		
		
		setTuples(tuples1);
		corpussize=getTuples().size();
		term_frequency = new ArrayList<HashMap<String,Integer>>(getTuples().size());
		inverse_DF = new HashMap<String,Integer>();
		weights = new ArrayList<HashMap<String,Double>>(getTuples().size()) ;
		calculate();
			
	}
	//will deal with quotes case but quotes must not be inside quotes
	//first line is schema spec. and will be discarded. However, it will be used
	//for determining numatts.
	public TFIDF(String file, boolean neglectFirst) throws IOException{
		Scanner in=new Scanner(new FileReader(new File(file)));
		ArrayList<String> tuples1=new ArrayList<String>();
		if(neglectFirst){
			if(in.hasNextLine()){
				numatts=(new CSVParser()).parseLine(in.nextLine()).length;
			
			}
		}
		else{
			while(in.hasNextLine())
				tuples1.add(in.nextLine().toLowerCase());
			numatts=(new CSVParser()).parseLine(tuples1.get(0)).length;
		}
		
		
		in.close();
		
		
		
		setTuples(tuples1);
		corpussize=getTuples().size();
		term_frequency = new ArrayList<HashMap<String,Integer>>(getTuples().size());
		inverse_DF = new HashMap<String,Integer>();
		weights = new ArrayList<HashMap<String,Double>>(getTuples().size()) ;
		calculate();
			
	}
	public void memoryop(){
		weights=null;
		term_frequency=null;
	}
	
	public void calculate()throws IOException{
		calculateTF();
		calculateIDF();
		calculateweights(); //use this for local 'N' IDF support
	}
	
	public void calculateweights(IDF a){
		if(!weightset)
			return;
		for(int i=0; i<getTuples().size(); i++){
			HashMap<String,Double> temp =new HashMap<String,Double>();
			double sum=0.0;
			for(String p: term_frequency.get(i).keySet()){
				sum+=(Math.log1p(term_frequency.get(i).get(p))*Math.log1p(a.corpsize*1.0/a.IF.get(p))/(Math.pow(Math.log(2), 2)));
				temp.put(p,Math.log1p(term_frequency.get(i).get(p))*Math.log1p(a.corpsize*1.0/a.IF.get(p))/(Math.pow(Math.log(2), 2)));
			}
			if(sum==0.0)
				weights.add(temp);
			else{
				for(String q: temp.keySet())
					temp.put(q, temp.get(q)/sum);
				weights.add(temp);
			}
		}
	}
	
	public void calculateweights(){
		if(weightset)
			return;
		for(int i=0; i<getTuples().size(); i++){
			HashMap<String,Double> temp =new HashMap<String,Double>();
			double sum=0.0;
			for(String p: term_frequency.get(i).keySet()){
				sum+=(Math.log1p(term_frequency.get(i).get(p))*Math.log1p(corpussize*1.0/inverse_DF.get(p))/(Math.pow(Math.log(2), 2)));
				temp.put(p,Math.log1p(term_frequency.get(i).get(p))*Math.log1p(corpussize*1.0/inverse_DF.get(p))/(Math.pow(Math.log(2), 2)));
			}
			if(sum==0.0)
				weights.add(temp);
			else{
				for(String q: temp.keySet())
					temp.put(q, temp.get(q)/sum);
				weights.add(temp);
			}
		}
	}
	
	public void calculateTF()throws IOException{
		for(int i=0; i<getTuples().size(); i++){
			String[] words=(new CSVParser()).parseLine(getTuples().get(i));
			HashMap<String,Integer> temp=new HashMap<String,Integer>();
			for(int j=0; j<words.length; j++){
				String[] tokens=words[j].split(Parameters.splitstring);
				for(int k=0; k<tokens.length; k++){
					tokens[k]=tokens[k].trim();
					if(tokens[k].length()==0)
						continue;
					if(temp.containsKey(tokens[k]))
						temp.put(tokens[k],temp.get(tokens[k])+1);
					else
						temp.put(tokens[k], 1);
				}
			}
			term_frequency.add(temp);
		}
	}
	
	public HashMap<String,Integer> calculateTF(String tuple)throws IOException{
		
			String[] words=(new CSVParser()).parseLine(tuple);
			HashMap<String,Integer> temp=new HashMap<String,Integer>();
			for(int j=0; j<words.length; j++){
				String[] tokens=words[j].split(Parameters.splitstring);
				for(int k=0; k<tokens.length; k++){
					tokens[k]=tokens[k].trim();
					if(tokens[k].length()==0)
						continue;
					if(temp.containsKey(tokens[k]))
						temp.put(tokens[k],temp.get(tokens[k])+1);
					else
						temp.put(tokens[k], 1);
				}
			}
			return temp;
	}
	
	public void calculateIDF(){
		
		for(int i=0; i<getTuples().size(); i++){
			HashSet<String> forbidden=new HashSet<String>();
			for(String p:term_frequency.get(i).keySet()){
				if(forbidden.contains(p))
					continue;
				else if(inverse_DF.containsKey(p))
					inverse_DF.put(p,inverse_DF.get(p)+1);
				else
					inverse_DF.put(p, 1);
				
				forbidden.add(p);
			}
		}
		
	}
	
	public double getScore(int i, int j){
		if(i>=corpussize||j>=corpussize)
			return 0.0;
		else{
			HashMap<String,Double> a=weights.get(i);
			
			double total=0.0;
			for(String t: weights.get(j).keySet())
				if(a.containsKey(t))
					total+=(a.get(t)*weights.get(j).get(t));
			return total;
		}
		
	}
	
	public double getScoreTF(int i, int j){
		if(i>=corpussize||j>=corpussize)
			return 0.0;
		else{
			HashMap<String,Integer> a=term_frequency.get(i);
			int total_a=computeTermSum(i);
			double total=0.0;
			for(String t: term_frequency.get(j).keySet())
				if(a.containsKey(t))
					total+=(a.get(t)*(1.0/total_a)*term_frequency.get(j).get(t)*(1.0/computeTermSum(j)));
			return total;
		}
		
	}
	
	
	//i is from this object, while j is from obj
	public double getScoreTF(TFIDF obj, int i, int j){
		if(i>=corpussize||j>=obj.corpussize)
			return 0.0;
		else{
			HashMap<String,Integer> a=term_frequency.get(i);
			int total_a=computeTermSum(i);
			double total=0.0;
			for(String t: obj.term_frequency.get(j).keySet())
				if(a.containsKey(t))
					total+=(a.get(t)*(1.0/total_a)*obj.term_frequency.get(j).get(t)*(1.0/obj.computeTermSum(j)));
			return total;
		}
		
	}
	
	public HashSet<Integer> pruneMap(TFIDF obj, int i, HashSet<Integer> j, double ut){
		HashSet<Integer> res=new HashSet<Integer>();
		for(int a:j)
			if(getScoreTF(obj,i,a)>=ut)
				res.add(a);
		return res;
	}
	
	
		public double getScoreTF(String tuple, int i)throws IOException{
			if(i>=corpussize)
				return 0.0;
			else{
				HashMap<String,Integer> a=term_frequency.get(i);
				int total_a=computeTermSum(i);
				double total=0.0;
				HashMap<String,Integer> tupletf=calculateTF(tuple);
				for(String t: tupletf.keySet())
					if(a.containsKey(t))
						total+=(a.get(t)*(1.0/total_a)*tupletf.get(t)*(1.0/computeTermSum(tupletf)));
				return total;
			}
			
		}
		
		
	
	private int computeTermSum(int i){
		int result=0;
		for(String q: term_frequency.get(i).keySet())
			result+=term_frequency.get(i).get(q);
		return result;
	}
	
	private int computeTermSum(HashMap<String,Integer> tf){
		int result=0;
		for(String q: tf.keySet())
			result+=tf.get(q);
		return result;
	}
	
	public ArrayList<String> getTuples() {
		return tuples;
	}
	public void setTuples(ArrayList<String> tuples) {
		this.tuples = tuples;
	}
	
	
}


