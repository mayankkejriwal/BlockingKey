package FeatureSelection;

import java.io.IOException;
import java.util.*;

import christen.Parameters;

public class LearnDisjunct {

	FeatureAnalysis feats;
	private static FeatureAnalysis tmp=null;
	
	int pos_label;	//refers to the class labels in Fisher
	int neg_label;
	
	public ArrayList<Integer> codes;
	 public ArrayList<Integer> attributes;
	 
	 public  ArrayList<ArrayList<Integer>> codesDNF;
	 public  ArrayList<ArrayList<Integer>> attributesDNF;
	 
	public ArrayList<Integer> bestFeats=new ArrayList<Integer>();
	
	HashSet<Integer> pos_forbidden;	//the positive examples that have been 'removed' from consideration
	HashSet<Integer> neg_forbidden; //the ones covered by more than gamma predicates
	HashSet<Integer> pred_forbidden; //the predicates that cover too many negative examples
	
	HashMap<Integer,Integer> pred_weights; //non-forbidden pred indexing to number of non-forbidden neg examples covered
	
	ArrayList<HashSet<Integer>> conjuncts=new ArrayList<HashSet<Integer>>();
	HashMap<HashSet<Integer>,Integer> inv_conjuncts=new HashMap<HashSet<Integer>,Integer>();
	
	int num_atts;
	int num_feats;
	
	
	private int num_eta=-1;
	
	private int epsilon=-1;
	
	
	
	
	public LearnDisjunct(FeatureAnalysis a, int pos, int neg, int num_att){
		if(!Parameters.DNF||(Parameters.DNF&&tmp==null)){
			feats=a;
			num_feats=feats.getA().get_num_features();
			if(Parameters.DNF)
				tmp=FeatureAnalysis.clone(a);
		}
		else
		{
			feats=FeatureAnalysis.clone(tmp);
			num_feats=feats.getA().get_num_features();
		}
		
		pos_label=pos;
		neg_label=neg;
		num_atts=num_att;
		codes=new ArrayList<Integer>();
		attributes=new ArrayList<Integer>();
		codesDNF=new ArrayList<ArrayList<Integer>>();
		attributesDNF=new ArrayList<ArrayList<Integer>>();
		pos_forbidden=new HashSet<Integer>();
		neg_forbidden=new HashSet<Integer>();
		pred_forbidden=new HashSet<Integer>();
		pred_weights=new HashMap<Integer,Integer>();
	}
	
	public LearnDisjunct(FeatureAnalysis a, int pos, int neg, int num_att, double recall){
		if(!Parameters.DNF||(Parameters.DNF&&tmp==null)){
			feats=a;
			num_feats=feats.getA().get_num_features();
			if(Parameters.DNF)
				tmp=FeatureAnalysis.clone(a);
		}
		else
		{
			feats=FeatureAnalysis.clone(tmp);
			num_feats=feats.getA().get_num_features();
		}
		pos_label=pos;
		neg_label=neg;
		num_atts=num_att;
		codes=new ArrayList<Integer>();
		attributes=new ArrayList<Integer>();
		codesDNF=new ArrayList<ArrayList<Integer>>();
		attributesDNF=new ArrayList<ArrayList<Integer>>();
		pos_forbidden=new HashSet<Integer>();
		neg_forbidden=new HashSet<Integer>();
		pred_forbidden=new HashSet<Integer>();
		pred_weights=new HashMap<Integer,Integer>();
		Parameters.recall=recall;
	}
	
	public void populateDisjunction_Features(){
		if(num_eta==-1)
			num_eta=(int) (Parameters.eta*feats.getA().features.get(neg_label).size());
			
			System.out.println("num_eta "+num_eta);
		
		if(epsilon==-1)
		epsilon=(int) ((1-Parameters.recall)*feats.getA().features.get(pos_label).size());
		
		if(num_eta==0)
			num_eta=1;
		//int posSize=feats.a.features.get(pos_label).size();
		//int negSize=feats.a.features.get(neg_label).size();
		int beta=feats.getA().features.get(pos_label).size();
		System.out.println("beta "+beta);
		for(int i=0; i<num_feats; i++)
			if(numNegCovered(i)>=num_eta)
				pred_forbidden.add(i);
		
		if(beta-checkPosCoverage()>epsilon){
			System.out.println("FAILURE: epsilon cannot be covered");
			return;
		}
		
		
		
		//populatePredWeights();
		
		int i=0;
		outer:
		while(beta-pos_forbidden.size()>=epsilon && i<feats.getScores().size()){
			
				Iterator<Integer> d=feats.getScoremap().get(feats.getScores().get(i)).iterator();
				while(d.hasNext()){
					int k=d.next();
					if(pred_forbidden.contains(k)||numPosCovered(k)==0
						//	||(numNegCovered(k)!=0&&numPosCoveredUnchecked(k)*negSize<numNegCovered(k)*posSize)
							)
						continue;
					
					addCode(k);
					bestFeats.add(k);
					pred_forbidden.add(k);
					
					updatePosForbidden(k);
					if(beta-pos_forbidden.size()<epsilon)
						break outer;
					
				}
				i++;
			}
			
		System.out.println("pos_forbidden.size "+pos_forbidden.size());
		
	}

	public void populateDisjunction_Bilenko(){
		if(num_eta==-1)
		num_eta=(int) (Parameters.eta*feats.getA().features.get(neg_label).size());
		
		
		System.out.println("num_eta "+num_eta);
		if(epsilon==-1)
		epsilon=(int) ((1-Parameters.recall)*feats.getA().features.get(pos_label).size());
		
		if(num_eta==0)
			num_eta=1;
		
		int beta=feats.getA().features.get(pos_label).size();
		//construct pred_forbidden
		for(int i=0; i<num_feats; i++)
			if(numNegCovered(i)>=num_eta)
				pred_forbidden.add(i);
		
		if(beta-checkPosCoverage()>epsilon){
			System.out.println("FAILURE: epsilon cannot be covered");
			return;
		}
		
		int gamma=(int) Math.sqrt((num_feats-pred_forbidden.size())/Math.log(beta));
		populateNegForbidden(gamma);
		
		populatePredWeights();
		
		while(beta-pos_forbidden.size()>=epsilon){
			double max=0.0;
			int pred=-1;
			for(int i:pred_weights.keySet()){
				int pos_cov=numPosCovered(i);
				if(pos_cov*1.0/pred_weights.get(i)>max){
					max=pos_cov*1.0/pred_weights.get(i);
					pred=i;
				}
			}
			if(pred!=-1){
				addCode(pred);
				pred_forbidden.add(pred);
				pred_weights.remove(pred);
				updatePosForbidden(pred);
			}
			else{
				System.out.println("pred: -1");
				break;
			}
		}
		
	}
	
	public void populateDNF_Bilenko(int k){
		if(num_eta==-1)
			num_eta=(int) (Parameters.eta*feats.getA().features.get(neg_label).size());
			
			
			System.out.println("num_eta "+num_eta);
			if(epsilon==-1)
			epsilon=(int) ((1-Parameters.recall)*feats.getA().features.get(pos_label).size());
			
			if(num_eta==0)
				num_eta=1;
			
			int beta=feats.getA().features.get(pos_label).size();
			//construct pred_forbidden
			for(int i=0; i<num_feats; i++)
				if(numNegCovered(i)>=num_eta)
					pred_forbidden.add(i);
			
			//System.out.println(num_feats);
			int conjunct_count=num_feats;
			int old_feats=num_feats;
			for(int i=0; i<num_feats;i++){
				if(pred_forbidden.contains(i))
					continue;
				ArrayList<Integer> conjunct=new ArrayList<Integer>();
				conjunct.add(i);
				for(int j=0; j<k-1; j++){
					int p=(int) getBest(conjunct);
					if(p==-1)
						break;
					else{
						conjunct.add(p);
						conjuncts.add(new HashSet<Integer>(conjunct));
						inv_conjuncts.put(new HashSet<Integer>(conjunct),conjunct_count);
						conjunct_count++;
					}
				
				}
			}
			//printconjuncts();
			
			
			updateFeatures();
			//feats.conjuncts=conjuncts;
			System.out.println(num_feats+" "+old_feats);
			
			if(beta-checkPosCoverage()>epsilon){
				System.out.println("FAILURE: epsilon cannot be covered");
				return;
			}
			
			int gamma=(int) Math.sqrt((num_feats-pred_forbidden.size())/Math.log(beta));
			populateNegForbidden(gamma);
			
			populatePredWeights();
			
			while(beta-pos_forbidden.size()>=epsilon){
				double max=0.0;
				int pred=-1;
				for(int i:pred_weights.keySet()){
					int pos_cov=numPosCovered(i);
					if(pos_cov*1.0/pred_weights.get(i)>max){
						max=pos_cov*1.0/pred_weights.get(i);
						pred=i;
					}
				}
				if(pred!=-1){
					addCodeDNFBilenko(pred, old_feats);
					pred_forbidden.add(pred);
					pred_weights.remove(pred);
					updatePosForbidden(pred);
				}
				else{
					System.out.println("pred: -1");
					break;
				}
			}
		//	printCodesDNF();
			
	}
	

	//int conjunctn has no use right now
	public void populateDNF_Features(int conjunctNum){
		if(num_eta==-1)
			num_eta=(int) (Parameters.eta*feats.getA().features.get(neg_label).size());
			
			System.out.println("num_eta "+num_eta);
		
		if(epsilon==-1)
		epsilon=(int) ((1-Parameters.recall)*feats.getA().features.get(pos_label).size());
		
		if(num_eta==0)
			num_eta=1;
		//int posSize=feats.a.features.get(pos_label).size();
		//int negSize=feats.a.features.get(neg_label).size();
		int beta=feats.getA().features.get(pos_label).size();
		System.out.println("beta "+beta);
		for(int i=0; i<feats.getFeats_num(); i++)
			if(numNegCovered(i)>=num_eta)
				pred_forbidden.add(i);
		
		if(beta-checkPosCoverage()>epsilon){
			System.out.println("FAILURE: epsilon cannot be covered");
			return;
		}
		
		
		feats.composeDNF(2);
		//populatePredWeights();
		
		int i=0;
		outer:
		while(beta-pos_forbidden.size()>=epsilon && i<feats.getScores().size()){
			
				Iterator<Integer> d=feats.getScoremap().get(feats.getScores().get(i)).iterator();
				while(d.hasNext()){
					int k=d.next();
					if(pred_forbidden.contains(k)||numPosCovered(k)==0
						//	||(numNegCovered(k)!=0&&numPosCoveredUnchecked(k)*negSize<numNegCovered(k)*posSize)
							)
						continue;
					
					addCodeDNF(k,feats.getA().get_num_features());
					bestFeats.add(k);
					pred_forbidden.add(k);
					
					updatePosForbidden(k);
					if(beta-pos_forbidden.size()<epsilon)
						break outer;
					
				}
				i++;
			}
			
		System.out.println("pos_forbidden.size "+pos_forbidden.size());
		
	}

	public ArrayList<String> codes()throws IOException{
		ArrayList<String> results=new ArrayList<String>();
		for(int i=0; i<codes.size(); i++){
			results.add(codes.get(i)+" "+attributes.get(i));
		}
		
		
		return results;
		
	}
	
	public ArrayList<String> codesDNF() throws IOException{
		ArrayList<String> results=new ArrayList<String>();
		for(int i=0; i<codesDNF.size(); i++){
			String tmp=new String("");
			for(int j=0; j<codesDNF.get(i).size()-1; j++)
				tmp=tmp+(codesDNF.get(i).get(j)+" "+attributesDNF.get(i).get(j)+" ");
			tmp=tmp+(codesDNF.get(i).get(codesDNF.get(i).size()-1)+" "+attributesDNF.get(i).get(codesDNF.get(i).size()-1));
			results.add(tmp);
		}
		
		
		return results;
	}
	
	private void updateFeatures(){
		feats.getA().set_num_features(feats.getA().get_num_features()
				+ conjuncts.size());
		for(int i: feats.getA().features.keySet()){
			ArrayList<ArrayList<Integer>> m=feats.getA().features.get(i);
			for(int j=0; j<m.size(); j++)
				for(int jj=0; jj<conjuncts.size(); jj++)
					if(conjunctionTrue(m.get(j),jj))
						m.get(j).add(1);
					else
						m.get(j).add(0);
			
		}
		feats.getA().reinitialize();
		feats.getA().computeStatistics();
		num_feats=feats.getA().get_num_features();
		feats.recompute(feats.getA());
	}
	
	private boolean conjunctionTrue(ArrayList<Integer> p, int c_index){
		Iterator<Integer> d=conjuncts.get(c_index).iterator();
		
		while(d.hasNext()){
			if((int)p.get(d.next())!=1)
				return false;
		}
		return true;
	}
	
	private void updatePosForbidden(int feat_index){
		ArrayList<ArrayList<Integer>> pos=feats.getA().features.get(pos_label);
		
		for(int i=0; i<pos.size(); i++)
			if(pos_forbidden.contains(i))
				continue;
			else if((int)pos.get(i).get(feat_index)==1)
				pos_forbidden.add(i);
		
	}
	
	private void addCodeDNF(int q, int old_feats){
		if(q<old_feats){
			ArrayList<Integer> tmp=new ArrayList<Integer>();
			tmp.add((q/num_atts)+1);
			codesDNF.add(tmp);
			ArrayList<Integer> tmp1=new ArrayList<Integer>();
			tmp1.add(q-((q/num_atts)*num_atts));
			attributesDNF.add(tmp1);
		}
		else {
			ArrayList<Integer> m=new ArrayList<Integer>(feats.getConjuncts().get(q-old_feats));
			ArrayList<Integer> tmp=new ArrayList<Integer>();
			ArrayList<Integer> tmp1=new ArrayList<Integer>();
			for(int i=0; i<m.size(); i++){
				int q1=m.get(i);
				tmp.add((q1/num_atts)+1);
				int q2=q1-((q1/num_atts)*num_atts);
				tmp1.add(q2);
				
			}
			codesDNF.add(tmp);
			attributesDNF.add(tmp1);
		}
		
	}
	
	private void addCodeDNFBilenko(int q, int old_feats){
		if(q<old_feats){
			ArrayList<Integer> tmp=new ArrayList<Integer>();
			tmp.add((q/num_atts)+1);
			codesDNF.add(tmp);
			ArrayList<Integer> tmp1=new ArrayList<Integer>();
			tmp1.add(q-((q/num_atts)*num_atts));
			attributesDNF.add(tmp1);
		}
		else {
			ArrayList<Integer> m=new ArrayList<Integer>(conjuncts.get(q-old_feats));
			ArrayList<Integer> tmp=new ArrayList<Integer>();
			ArrayList<Integer> tmp1=new ArrayList<Integer>();
			for(int i=0; i<m.size(); i++){
				int q1=m.get(i);
				tmp.add((q1/num_atts)+1);
				int q2=q1-((q1/num_atts)*num_atts);
				tmp1.add(q2);
				
			}
			codesDNF.add(tmp);
			attributesDNF.add(tmp1);
		}
		
	}


	private void addCode(int q){
		codes.add((q/num_atts)+1);
		attributes.add(q-((q/num_atts)*num_atts));
	}
	
	//possibility: division by 0. That's why we add one to all the counts
	private void populatePredWeights(){
		for(int i=0; i<num_feats; i++)
			if(pred_forbidden.contains(i))
				continue;
			else{
				int count=0;
				for(int j=0; j<feats.getA().features.get(neg_label).size();j++)
					if(neg_forbidden.contains(j))
						continue;
					else if((int)feats.getA().features.get(neg_label).get(j).get(i)==1)
						count++;
				
				pred_weights.put(i, count+1);
			}
	}
	
	private void populateNegForbidden(int gamma){
		ArrayList<ArrayList<Integer>> neg=feats.getA().features.get(neg_label);
		for(int i=0; i<neg.size(); i++){
			int count=0;
			for(int j=0; j<neg.get(i).size()&&count<=gamma; j++){
				if(pred_forbidden.contains(j))
					continue;
				else if((int)neg.get(i).get(j)==1)
					count++;
			}
			if(count>gamma)
				neg_forbidden.add(i);
		}
	}
	
	private int numPosCovered(int feat_index){
		ArrayList<ArrayList<Integer>> pos=feats.getA().features.get(pos_label);
		int count=0;
		for(int i=0; i<pos.size(); i++)
			if(pos_forbidden.contains(i))
				continue;
			else if((int)pos.get(i).get(feat_index)==1)
				count++;
		return count;
	}
	
	private int numNegCovered(int feat_index){
		ArrayList<ArrayList<Integer>> neg=feats.getA().features.get(neg_label);
		int count=0;
		for(int i=0; i<neg.size(); i++)
			 if((int)neg.get(i).get(feat_index)==1)
				count++;
		return count;
	}
	
	private int numPosCovered(ArrayList<Integer> feat_index){
		ArrayList<ArrayList<Integer>> pos=feats.getA().features.get(pos_label);
		int count=0;
		for(int i=0; i<pos.size(); i++)
			if(pos_forbidden.contains(i))
				continue;
			else 
			{	int flag=0;
				for(int k=0; k<feat_index.size(); k++)
					if((int)pos.get(i).get(feat_index.get(k))!=1)
						{flag=-1; break;}
				if(flag==0)
					count++;
			}
				
		return count;
	}
	
	private int numNegCovered(ArrayList<Integer> feat_index){
		ArrayList<ArrayList<Integer>> neg=feats.getA().features.get(neg_label);
		int count=0;
		for(int i=0; i<neg.size(); i++){
			int flag=0;
			for(int k=0; k<feat_index.size(); k++)
			 if((int)neg.get(i).get(feat_index.get(k))!=1)
			 {flag=-1; break;}
			if(flag==0)
				count++;
		}
		return count;
	}
	
	private int getBest(ArrayList<Integer> conjunct){
		HashSet<Integer> d=new HashSet<Integer>(conjunct);
		int currentBest=-1;
		double max=0.0;
		for(int i=0; i<num_feats; i++)
			if(d.contains(i)||pred_forbidden.contains(i))
				continue;
			else{
				d.add(i);
				if(inv_conjuncts.containsKey(d))
				{
					d.remove(i);
					continue;
				}
				else{
					//we add 1 to the denom. to prevent division by 0
					double temp=1.0*numPosCovered(new ArrayList<Integer>(d))/(numNegCovered(new ArrayList<Integer>(d))+1);
					if(temp>=max){
						max=temp;
						currentBest=i;
						
					}
					d.remove(i);
				}
			}
		return currentBest;
		
	}
	
	//checks how many positive examples are covered by non-forbidden predicates
	private int checkPosCoverage(){
		ArrayList<ArrayList<Integer>> pos=feats.getA().features.get(pos_label);
		int count=0;
		for(int i=0; i<pos.size(); i++)
			for(int j=0; j<pos.get(i).size(); j++)
				if(pred_forbidden.contains(j))
					continue;
				else if((int)pos.get(i).get(j)==1){
					count++;
					break;
				}
			return count;
	}
	
}
