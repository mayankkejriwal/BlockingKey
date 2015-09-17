package christen;

import java.io.IOException;
import java.util.*;



public class DNFBlocking {
	
	TFIDF db;
	ArrayList<String> tuples;
	HashMap<Integer,TFIDF> tokenTFIDF=new HashMap<Integer,TFIDF>();
	HashMap<Integer,TFIDF> threegramTFIDF=new HashMap<Integer,TFIDF>();	//key is the attribute on which this is applied
	HashMap<Integer,TFIDF> fivegramTFIDF=new HashMap<Integer,TFIDF>();
	FindUnsupervisedDuplicates d;
	
	ArrayList<Integer> codes;
	ArrayList<Integer> attributes;
	ArrayList<ArrayList<Integer>> codes_DNF;
	ArrayList<ArrayList<Integer>> attributes_DNF;
	HashMap<Integer,HashSet<Integer>> dup1_index;	//indexes in dup1
	HashMap<Integer,HashSet<Integer>> dup2_index;
	ArrayList<Integer> dup1;	//will always contain the smaller index
	ArrayList<Integer> dup2;
	ArrayList<HashMap<String,HashSet<Integer>>> dnf_blocks;
	
	public static double RR=0.0; //reduction ratio
	public static int PCount=0; //pairs count
	public static double Pcomp=0.0; //pairs completeness
	public static double PQ=0.0; //pairs quality
	public static int BKs=0; //number of blocking keys
	
	
	

	public DNFBlocking(String relation)throws IOException{
		db=new TFIDF(relation);
		codes=new ArrayList<Integer>();
		attributes=new ArrayList<Integer>();
		codes_DNF=new ArrayList<ArrayList<Integer>>();
		attributes_DNF=new ArrayList<ArrayList<Integer>>();
		tuples=db.getTuples();
		dnf_blocks=new ArrayList<HashMap<String,HashSet<Integer>>>();
		dup1_index=new HashMap<Integer,HashSet<Integer>>();
		dup2_index=new HashMap<Integer,HashSet<Integer>>();
		dup1=new ArrayList<Integer>();
		dup2=new ArrayList<Integer>();
		
		RR=0;
		PCount=0;
		Pcomp=0;
		PQ=0;
		BKs=0;
	}
	
	public void processDNF(ArrayList<String> codesDNF, String goldstandards, char c)throws IOException{
		setCodesDNF(codesDNF);
		for(int i=0; i<codes_DNF.size(); i++){
			if(codes_DNF.get(i).size()>1){
			System.out.println(codes_DNF.get(i));
			System.out.println(attributes_DNF.get(i));
			}
		}
		for(int i=0; i<codes_DNF.size(); i++){
			System.out.println("Blocking "+i);
			BKs++;
			blockAttributeDNF(i);
		}
		
		generateDuplicates();
		
		printDataSetResults(goldstandards, c);
		
	}
	
	public void process(ArrayList<String> codes, String goldstandards, char c)throws IOException{
		setCodes(codes);
		
		for(int i=0; i<codes.size(); i++){
			System.out.println("Blocking "+i);
			BKs++;
			blockAttribute(i);
		}
		
		generateDuplicates();
		
		printDataSetResults(goldstandards, c);
	}
	
	public boolean containsDuplicate(int a, int b){
		int min=b;
		int max=a;
		if(a<b){
			min=a;
			max=b;
		}
		return HashSetIntersect(dup1_index.get(min),dup2_index.get(max));
	}
	
	private void setCodesDNF(ArrayList<String> codesDNF)throws IOException{
		Iterator<String> in=codesDNF.iterator();
		ArrayList<ArrayList<Integer>> c=new ArrayList<ArrayList<Integer>>();
		ArrayList<ArrayList<Integer>> a=new ArrayList<ArrayList<Integer>>();
		while(in.hasNext()){
			String[] t=in.next().split(" ");
			if(t.length%2!=0)
				System.out.println("ERROR IN CODE FILE!");
			else
			{
				ArrayList<Integer> cc=new ArrayList<Integer>();
				ArrayList<Integer> aa=new ArrayList<Integer>();
				for(int i=0; i<t.length; i+=2){
				cc.add(Integer.parseInt(t[i]));
				aa.add(Integer.parseInt(t[i+1]));
				}
				c.add(cc);
				a.add(aa);
			}
		}
		
		setCodesDNF(c,a);
	}
	
	private void setCodesDNF(ArrayList<ArrayList<Integer>> c, ArrayList<ArrayList<Integer>> a)throws IOException{
		codes_DNF=c;
		attributes_DNF=a;
		
		for(int m=0; m<codes_DNF.size(); m++){
			ArrayList<Integer> codes=codes_DNF.get(m);
			ArrayList<Integer> attributes=attributes_DNF.get(m);
			for(int i=0; i<codes.size(); i++)
				if((int)codes.get(i)>=11&&(int)codes.get(i)<=15){
					if(tokenTFIDF.containsKey(attributes.get(i)))
						continue;
					ArrayList<String> fields=new ArrayList<String>();
					for(int j=0; j<tuples.size(); j++)
						fields.add(tuples.get(j).split(",")[attributes.get(i)]);
					tokenTFIDF.put(attributes.get(i), new TFIDF(fields));
				}
				else if((int)codes.get(i)>15&&(int)codes.get(i)<=20){
					if(threegramTFIDF.containsKey(attributes.get(i)))
						continue;
					ArrayList<String> tokens=new ArrayList<String>();
					for(int j=0; j<tuples.size(); j++)
						tokens.add(constructNgrams(tuples.get(j).split(",")[attributes.get(i)],3));
					threegramTFIDF.put(attributes.get(i), new TFIDF(tokens));
				}
				else if((int)codes.get(i)>20&&(int)codes.get(i)<=25){
					if(fivegramTFIDF.containsKey(attributes.get(i)))
						continue;
					ArrayList<String> tokens=new ArrayList<String>();
					for(int j=0; j<tuples.size(); j++)
						tokens.add(constructNgrams(tuples.get(j).split(",")[attributes.get(i)],5));
					fivegramTFIDF.put(attributes.get(i), new TFIDF(tokens));
				}
			
		}
		
		
	}
	

	private void setCodes(ArrayList<String> codes)throws IOException{
		Iterator<String> in=codes.iterator();
		ArrayList<Integer> c=new ArrayList<Integer>();
		ArrayList<Integer> a=new ArrayList<Integer>();
		while(in.hasNext()){
			String[] t=in.next().split(" ");
			if(t.length!=2)
				System.out.println("ERROR IN CODE FILE!");
			else
			{
				c.add(Integer.parseInt(t[0]));
				a.add(Integer.parseInt(t[1]));
			}
		}
		
		setCodes(c,a);
	}
	
	private void setCodes(ArrayList<Integer> c, ArrayList<Integer> a)throws IOException{
		codes=c;
		attributes=a;
		
		for(int i=0; i<codes.size(); i++)
			if((int)codes.get(i)>=11&&(int)codes.get(i)<=15){
				if(tokenTFIDF.containsKey(attributes.get(i)))
					continue;
				ArrayList<String> fields=new ArrayList<String>();
				for(int j=0; j<tuples.size(); j++)
					fields.add(tuples.get(j).split(",")[attributes.get(i)]);
				tokenTFIDF.put(attributes.get(i), new TFIDF(fields));
			}
			else if((int)codes.get(i)>15&&(int)codes.get(i)<=20){
				if(threegramTFIDF.containsKey(attributes.get(i)))
					continue;
				ArrayList<String> tokens=new ArrayList<String>();
				for(int j=0; j<tuples.size(); j++)
					tokens.add(constructNgrams(tuples.get(j).split(",")[attributes.get(i)],3));
				threegramTFIDF.put(attributes.get(i), new TFIDF(tokens));
			}
			else if((int)codes.get(i)>20&&(int)codes.get(i)<=25){
				if(fivegramTFIDF.containsKey(attributes.get(i)))
					continue;
				ArrayList<String> tokens=new ArrayList<String>();
				for(int j=0; j<tuples.size(); j++)
					tokens.add(constructNgrams(tuples.get(j).split(",")[attributes.get(i)],5));
				fivegramTFIDF.put(attributes.get(i), new TFIDF(tokens));
			}
		
		
	}
	
	//send in order! Do not mix usage with blockAttribute, only one should be used
	private void blockAttributeDNF(int index){
		
		if(codes_DNF.get(index).size()==1){
			dnf_blocks.add(blockAttributeDNFHelp(codes_DNF.get(index).get(0),attributes_DNF.get(index).get(0)));
			return;
		}
		
		HashMap<String,HashSet<Integer>> fin=new HashMap<String,HashSet<Integer>>();
		fin=blockAttributeDNFHelp(codes_DNF.get(index).get(0),attributes_DNF.get(index).get(0));
	
		ArrayList<Integer> codes=codes_DNF.get(index);
		ArrayList<Integer> attributes=attributes_DNF.get(index);
		
		for(int i=1; i<codes.size(); i++){
			HashMap<String, HashSet<Integer>> current=blockAttributeDNFHelp(codes.get(i),attributes.get(i));
			fin=mergeMaps(fin,current);
		}
		dnf_blocks.add(fin);
	}
	
	//will merge a and b into a, blocking key style, using comma as delimiter
	private HashMap<String,HashSet<Integer>> mergeMaps(HashMap<String,HashSet<Integer>> a, HashMap<String,HashSet<Integer>> b){
		HashMap<String, HashSet<Integer>> result=new HashMap<String,HashSet<Integer>>();
		for(String p: a.keySet()){
			
			for(String q:b.keySet()){
				HashSet<Integer> l=HashSetIntersect2(a.get(p),b.get(q));
				if(l!=null&&l.size()!=0)
					result.put(p+","+q, l);
			}
		}
		
		return result;
	}
	
	
	//refer to the codes/attributes index. will simply add to dnf_blocks so send in order.
	private HashMap<String,HashSet<Integer>> blockAttributeDNFHelp(int code, int attribute){
		HashMap<String,HashSet<Integer>> hash=new HashMap<String,HashSet<Integer>>();
		
		if(code==1){
			for(int i=0; i<tuples.size(); i++){
				String s=tuples.get(i).split(",")[attribute];
				if(hash.containsKey(s))
					hash.get(s).add(i);
				else{
					hash.put(s, new HashSet<Integer>());
					hash.get(s).add(i);
				}
			}
		}
		else if(code==2){
			for(int i=0; i<tuples.size(); i++){
				String s=tuples.get(i).split(",")[attribute];
				String[] d=s.split(Parameters.splitstring);
				for(int j=0; j<d.length; j++)
				if(hash.containsKey(d[j]))
					hash.get(d[j]).add(i);
				else{
					hash.put(d[j], new HashSet<Integer>());
					hash.get(d[j]).add(i);
				}
			}
			
		}
		else if(code==3){
			for(int i=0; i<tuples.size(); i++){
				String s=tuples.get(i).split(",")[attribute];
				String[] d=s.split(Parameters.splitstring);
				for(int j=0; j<d.length; j++){
					if(!isInteger(d[j]))
						continue;
					else{
						int q=Integer.parseInt(d[j]);
						d[j]=Integer.toString(q);
					}
					if(hash.containsKey(d[j]))
						hash.get(d[j]).add(i);
					else{
						hash.put(d[j], new HashSet<Integer>());
						hash.get(d[j]).add(i);
					}
				}
			}
		}
			else if(code==4){
				for(int i=0; i<tuples.size(); i++){
					String s=tuples.get(i).split(",")[attribute];
					String[] d=s.split(Parameters.splitstring);
					for(int j=0; j<d.length; j++){
						if(!isInteger(d[j]))
							continue;
						else{
							int q=Integer.parseInt(d[j]);
							String q1=Integer.toString(q-1);
							String q2=Integer.toString(q+1);
							if(hash.containsKey(q1))
								hash.get(q1).add(i);
							if(hash.containsKey(q2))
								hash.get(q2).add(i);
							
							d[j]=Integer.toString(q);
						}
						if(hash.containsKey(d[j]))
							hash.get(d[j]).add(i);
						else{
							hash.put(d[j], new HashSet<Integer>());
							hash.get(d[j]).add(i);
						}
					}
				}
			}
			else if(code==5||code==6||code==7){
				int n=3;
				if(code==6)
					n=5;
				else if(code==7)
					n=7;
				for(int i=0; i<tuples.size(); i++){
					String s=tuples.get(i).split(",")[attribute];
					String[] d=s.split(Parameters.splitstring);
					for(int j=0; j<d.length; j++){
						if(n<=d[j].length())
							d[j]=d[j].substring(0,n);
						if(hash.containsKey(d[j]))
							hash.get(d[j]).add(i);
						else{
								hash.put(d[j], new HashSet<Integer>());
								hash.get(d[j]).add(i);
							}
						
							
						}
				}
				
			}
			else if(code==8||code==9||code==10){
				int n=2;
				if(code==9)
					n=4;
				else if(code==10)
					n=6;
				for(int i=0; i<tuples.size(); i++){
					String s=tuples.get(i).split(",")[attribute];
					String[] d=s.split(Parameters.splitstring);
					if(d.length<n){
						String t=new String("");
						for(int k=0; k<=d.length-1; k++)
							t+=(d[k]+" ");
						if(t.length()>0)
						t=t.substring(0,t.length()-1);
						if(hash.containsKey(t))
							hash.get(t).add(i);
						else{
								hash.put(t, new HashSet<Integer>());
								hash.get(t).add(i);
							}
					}
					else
					for(int j=0; j<=d.length-n; j++){
						String t=new String("");
						for(int k=j; k<j+n; k++)
							t+=(d[k]+" ");
						if(t.length()>0)
						t=t.substring(0,t.length()-1);
						
						if(hash.containsKey(t))
							hash.get(t).add(i);
						else{
								hash.put(t, new HashSet<Integer>());
								hash.get(t).add(i);
							}
						
							
						}
				}
				
			}
			else if(code>=11&&code<=15){
				double delta=0.2*((code-11)+1);
				HashMap<String,Integer> bkv=new HashMap<String,Integer>();
				for(int i=0; i<tuples.size(); i++){
					String s=tuples.get(i).split(",")[attribute];
					if(bkv.containsKey(s)){
						hash.get(s).add(i);
						
						
					}
					else{
						bkv.put(s, i);
						hash.put(s, new HashSet<Integer>());
						hash.get(s).add(i);
					}
					for(String k:bkv.keySet()){
						if(k.equals(s))
							continue;
						HashMap<String,Double> q1=tokenTFIDF.get(attribute).weights.get(bkv.get(k));
						HashMap<String,Double> q2=tokenTFIDF.get(attribute).weights.get(i);
						if(tokenTFIDF(q1,q2,delta))
							hash.get(k).add(i);
					}
				}
			}
			else if(code>=16&&code<=20){
				double delta=0.2*((code-16)+1);
				HashMap<String,Integer> bkv=new HashMap<String,Integer>();
				for(int i=0; i<tuples.size(); i++){
					String s=tuples.get(i).split(",")[attribute];
					s=constructNgrams(s,3);
					if(bkv.containsKey(s)){
						hash.get(s).add(i);
						
						
					}
					else{
						bkv.put(s, i);
						hash.put(s, new HashSet<Integer>());
						hash.get(s).add(i);
					}
					for(String k:bkv.keySet()){
						if(k.equals(s))
							continue;
						HashMap<String,Double> q1=threegramTFIDF.get(attribute).weights.get(bkv.get(k));
						HashMap<String,Double> q2=threegramTFIDF.get(attribute).weights.get(i);
						if(tokenTFIDF(q1,q2,delta))
							hash.get(k).add(i);
					}
				}
			}
			else if(code>=21&&code<=25){
				double delta=0.2*((code-21)+1);
				HashMap<String,Integer> bkv=new HashMap<String,Integer>();
				for(int i=0; i<tuples.size(); i++){
					String s=tuples.get(i).split(",")[attribute];
					s=constructNgrams(s,5);
					if(bkv.containsKey(s)){
						hash.get(s).add(i);
						
						
					}
					else{
						bkv.put(s, i);
						hash.put(s, new HashSet<Integer>());
						hash.get(s).add(i);
					}
					for(String k:bkv.keySet()){
						if(k.equals(s))
							continue;
						HashMap<String,Double> q1=fivegramTFIDF.get(attribute).weights.get(bkv.get(k));
						HashMap<String,Double> q2=fivegramTFIDF.get(attribute).weights.get(i);
						if(tokenTFIDF(q1,q2,delta))
							hash.get(k).add(i);
					}
				}
			}
			
		
		
		return hash;
	}

	//refer to the codes/attributes index. will simply add to dnf_blocks so send in order.
	private void blockAttribute(int index){
		HashMap<String,HashSet<Integer>> hash=new HashMap<String,HashSet<Integer>>();
		int code=codes.get(index);
		if(code==1){
			for(int i=0; i<tuples.size(); i++){
				String s=tuples.get(i).split(",")[attributes.get(index)];
				if(hash.containsKey(s))
					hash.get(s).add(i);
				else{
					hash.put(s, new HashSet<Integer>());
					hash.get(s).add(i);
				}
			}
		}
		else if(code==2){
			for(int i=0; i<tuples.size(); i++){
				String s=tuples.get(i).split(",")[attributes.get(index)];
				String[] d=s.split(Parameters.splitstring);
				for(int j=0; j<d.length; j++)
				if(hash.containsKey(d[j]))
					hash.get(d[j]).add(i);
				else{
					hash.put(d[j], new HashSet<Integer>());
					hash.get(d[j]).add(i);
				}
			}
			
		}
		else if(code==3){
			for(int i=0; i<tuples.size(); i++){
				String s=tuples.get(i).split(",")[attributes.get(index)];
				String[] d=s.split(Parameters.splitstring);
				for(int j=0; j<d.length; j++){
					if(!isInteger(d[j]))
						continue;
					else{
						int q=Integer.parseInt(d[j]);
						d[j]=Integer.toString(q);
					}
					if(hash.containsKey(d[j]))
						hash.get(d[j]).add(i);
					else{
						hash.put(d[j], new HashSet<Integer>());
						hash.get(d[j]).add(i);
					}
				}
			}
		}
			else if(code==4){
				for(int i=0; i<tuples.size(); i++){
					String s=tuples.get(i).split(",")[attributes.get(index)];
					String[] d=s.split(Parameters.splitstring);
					for(int j=0; j<d.length; j++){
						if(!isInteger(d[j]))
							continue;
						else{
							int q=Integer.parseInt(d[j]);
							String q1=Integer.toString(q-1);
							String q2=Integer.toString(q+1);
							if(hash.containsKey(q1))
								hash.get(q1).add(i);
							if(hash.containsKey(q2))
								hash.get(q2).add(i);
							
							d[j]=Integer.toString(q);
						}
						if(hash.containsKey(d[j]))
							hash.get(d[j]).add(i);
						else{
							hash.put(d[j], new HashSet<Integer>());
							hash.get(d[j]).add(i);
						}
					}
				}
			}
			else if(code==5||code==6||code==7){
				int n=3;
				if(code==6)
					n=5;
				else if(code==7)
					n=7;
				for(int i=0; i<tuples.size(); i++){
					String s=tuples.get(i).split(",")[attributes.get(index)];
					String[] d=s.split(Parameters.splitstring);
					for(int j=0; j<d.length; j++){
						if(n<=d[j].length())
							d[j]=d[j].substring(0,n);
						if(hash.containsKey(d[j]))
							hash.get(d[j]).add(i);
						else{
								hash.put(d[j], new HashSet<Integer>());
								hash.get(d[j]).add(i);
							}
						
							
						}
				}
				
			}
			else if(code==8||code==9||code==10){
				int n=2;
				if(code==9)
					n=4;
				else if(code==10)
					n=6;
				for(int i=0; i<tuples.size(); i++){
					String s=tuples.get(i).split(",")[attributes.get(index)];
					String[] d=s.split(Parameters.splitstring);
					if(d.length<n){
						String t=new String("");
						for(int k=0; k<=d.length-1; k++)
							t+=(d[k]+" ");
						if(t.length()>0)
						t=t.substring(0,t.length()-1);
						if(hash.containsKey(t))
							hash.get(t).add(i);
						else{
								hash.put(t, new HashSet<Integer>());
								hash.get(t).add(i);
							}
					}
					else
					for(int j=0; j<=d.length-n; j++){
						String t=new String("");
						for(int k=j; k<j+n; k++)
							t+=(d[k]+" ");
						if(t.length()>0)
						t=t.substring(0,t.length()-1);
						
						if(hash.containsKey(t))
							hash.get(t).add(i);
						else{
								hash.put(t, new HashSet<Integer>());
								hash.get(t).add(i);
							}
						
							
						}
				}
				
			}
			else if(code>=11&&code<=15){
				double delta=0.2*((code-11)+1);
				HashMap<String,Integer> bkv=new HashMap<String,Integer>();
				for(int i=0; i<tuples.size(); i++){
					String s=tuples.get(i).split(",")[attributes.get(index)];
					if(bkv.containsKey(s)){
						hash.get(s).add(i);
						
						
					}
					else{
						bkv.put(s, i);
						hash.put(s, new HashSet<Integer>());
						hash.get(s).add(i);
					}
					for(String k:bkv.keySet()){
						if(k.equals(s))
							continue;
						HashMap<String,Double> q1=tokenTFIDF.get(attributes.get(index)).weights.get(bkv.get(k));
						HashMap<String,Double> q2=tokenTFIDF.get(attributes.get(index)).weights.get(i);
						if(tokenTFIDF(q1,q2,delta))
							hash.get(k).add(i);
					}
				}
			}
			else if(code>=16&&code<=20){
				double delta=0.2*((code-16)+1);
				HashMap<String,Integer> bkv=new HashMap<String,Integer>();
				for(int i=0; i<tuples.size(); i++){
					String s=tuples.get(i).split(",")[attributes.get(index)];
					s=constructNgrams(s,3);
					if(bkv.containsKey(s)){
						hash.get(s).add(i);
						
						
					}
					else{
						bkv.put(s, i);
						hash.put(s, new HashSet<Integer>());
						hash.get(s).add(i);
					}
					for(String k:bkv.keySet()){
						if(k.equals(s))
							continue;
						HashMap<String,Double> q1=threegramTFIDF.get(attributes.get(index)).weights.get(bkv.get(k));
						HashMap<String,Double> q2=threegramTFIDF.get(attributes.get(index)).weights.get(i);
						if(tokenTFIDF(q1,q2,delta))
							hash.get(k).add(i);
					}
				}
			}
			else if(code>=21&&code<=25){
				double delta=0.2*((code-21)+1);
				HashMap<String,Integer> bkv=new HashMap<String,Integer>();
				for(int i=0; i<tuples.size(); i++){
					String s=tuples.get(i).split(",")[attributes.get(index)];
					s=constructNgrams(s,5);
					if(bkv.containsKey(s)){
						hash.get(s).add(i);
						
						
					}
					else{
						bkv.put(s, i);
						hash.put(s, new HashSet<Integer>());
						hash.get(s).add(i);
					}
					for(String k:bkv.keySet()){
						if(k.equals(s))
							continue;
						HashMap<String,Double> q1=fivegramTFIDF.get(attributes.get(index)).weights.get(bkv.get(k));
						HashMap<String,Double> q2=fivegramTFIDF.get(attributes.get(index)).weights.get(i);
						if(tokenTFIDF(q1,q2,delta))
							hash.get(k).add(i);
					}
				}
			}
			
		
		
		dnf_blocks.add(hash);
	}
	//call this after dnf_blocking has been populated
		private void generateDuplicates(){
			for(int i=0; i<dnf_blocks.size(); i++){
				for(String p:dnf_blocks.get(i).keySet()){
					ArrayList<Integer> a=new ArrayList<Integer>(dnf_blocks.get(i).get(p));
					Collections.sort(a);
					for(int j=0; j<a.size()-1; j++)
						for(int k=j+1;k<a.size();k++){
							int min=a.get(k);
							int max=a.get(j);
							if((int)a.get(j)<(int)a.get(k)){
								min=a.get(j);
								max=a.get(k);
							}
							if(!HashSetIntersect(dup1_index.get(min),dup2_index.get(max))){
								dup1.add(min);
								dup2.add(max);
								if(dup1_index.containsKey(min))
									dup1_index.get(min).add(dup1.size()-1);
								else
								{
									dup1_index.put(min, new HashSet<Integer>());
									dup1_index.get(min).add(dup1.size()-1);
								}
								
								if(dup2_index.containsKey(max))
									dup2_index.get(max).add(dup2.size()-1);
								else
								{
									dup2_index.put(max, new HashSet<Integer>());
									dup2_index.get(max).add(dup2.size()-1);
								}
							}
						}
				}
			}
		}
		
		private void printDataSetResults(String goldstandards, char dataset)throws IOException{
			d=new FindUnsupervisedDuplicates(dataset);
			d.importGoldStandard(goldstandards);
			int numtuples=db.getTuples().size();
			double rr=1.0-(dup1.size()/(numtuples*0.5*(numtuples-1)));
			System.out.println("Reduction Ratio: "+rr);
			RR=rr;
			int count=0;
			for(int i=0;i<dup1.size(); i++)
				if(d.contains(dup1.get(i), dup2.get(i)))
					count++;
			
			System.out.println("pairs count "+count);
			PCount=count;
			System.out.println("Pairs Completeness: "+count*1.0/d.num_dups);
			Pcomp=count*1.0/d.num_dups;
			System.out.println("Pairs Quality: "+count*1.0/dup1.size());	
			PQ=count*1.0/dup1.size();
		}
		
		private boolean tokenTFIDF(HashMap<String,Double> w1,HashMap<String,Double> w2, double delta){
		
			double sum=0.0;
			for(String p:w1.keySet())
				if(w2.containsKey(p))
					sum+=(w1.get(p)*w2.get(p));
			
			if(sum>=delta)
				return true;
			else
				return false;
		
		
	}
	private static boolean isInteger(String s) {
	    try { 
	        Integer.parseInt(s); 
	    } catch(NumberFormatException e) { 
	        return false; 
	    }
	    // only got here if we didn't return false
	    return true;
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
	private boolean HashSetIntersect(HashSet<Integer> a,HashSet<Integer> b){
		if(a==null||b==null)
			return false;
		Iterator<Integer> q=a.iterator();
		while(q.hasNext()){
			if(b.contains(q.next()))
				return true;
			
		}
		return false;
	}
	
	private HashSet<Integer> HashSetIntersect2(HashSet<Integer> a,HashSet<Integer> b){
		if(a==null||b==null)
			return null;
		else if(a.size()==0||b.size()==0)
			return new HashSet<Integer>();
		Iterator<Integer> q=a.iterator();
		HashSet<Integer> result=new HashSet<Integer>();
		while(q.hasNext()){
			int m=q.next();
			if(b.contains(m))
				result.add(m);
			
		}
		return result;
	}
	
	

}
