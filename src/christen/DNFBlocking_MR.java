package christen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;

public class DNFBlocking_MR {
	
	
	
	
	
	public ArrayList<ArrayList<Integer>> codes_DNF;
	public ArrayList<ArrayList<Integer>> attributes_DNF;
	public int num_clauses=-1;
	String tuple;
	
	

	public DNFBlocking_MR(ArrayList<String> BK, String line)throws IOException{
		if(line!=null)
			tuple=new String(line.toLowerCase());
		codes_DNF=new ArrayList<ArrayList<Integer>>();
		attributes_DNF=new ArrayList<ArrayList<Integer>>();
		
		Iterator<String> in=BK.iterator();
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
				codes_DNF.add(cc);
				attributes_DNF.add(aa);
			}
		}
		num_clauses=codes_DNF.size();
		
	}
	
	public void setLine(String line){
		tuple=line;
	}
	
	private HashSet<String> blockHelp(int code, int attribute){
		String attr=tuple.split(",")[attribute];
		HashSet<String> res=new HashSet<String>();
		
		if(code==1)
			res.add(attr);
		else if(code==2)
			{
			String[] tokens=attr.split(Parameters.splitstring);
			for(int i=0; i<tokens.length; i++)
				res.add(tokens[i]);
			}
		else if(code==3)
		{
			String[] tokens=attr.split(Parameters.splitstring);
			for(int i=0; i<tokens.length; i++)
				if(isInteger(tokens[i]))
					res.add(tokens[i]);
			}
		else if(code==4)
		{
			String[] tokens=attr.split(Parameters.splitstring);
			for(int i=0; i<tokens.length; i++)
				if(isInteger(tokens[i])){
					Integer q=new Integer(Integer.parseInt(tokens[i]));
					Integer p=new Integer(Integer.parseInt(tokens[i])-1);
					Integer r=new Integer(Integer.parseInt(tokens[i])+1);
					res.add(q.toString());
					res.add(p.toString());
					res.add(r.toString());
				}
			}
		else if(code==5)
		{
			String[] tokens=attr.split(Parameters.splitstring);
			for(int i=0; i<tokens.length; i++)
				if(tokens[i].length()<3)
					res.add(tokens[i]);
				else res.add(tokens[i].substring(0,3));
			}
		else if(code==6)
		{
			String[] tokens=attr.split(Parameters.splitstring);
			for(int i=0; i<tokens.length; i++)
				if(tokens[i].length()<5)
					res.add(tokens[i]);
				else res.add(tokens[i].substring(0,5));
			}
		else if(code==7)
		{
			String[] tokens=attr.split(Parameters.splitstring);
			for(int i=0; i<tokens.length; i++)
				if(tokens[i].length()<7)
					res.add(tokens[i]);
				else res.add(tokens[i].substring(0,7));
			}
		else if(code==8){
			String[] tokens=attr.split(Parameters.splitstring);
			for(int i=0; i<tokens.length-1; i++)
				res.add(tokens[i]+"_"+tokens[i+1]);
		}
			
		else if(code==9)
		{
			String[] tokens=attr.split(Parameters.splitstring);
			for(int i=0; i<tokens.length-3; i++)
				res.add(tokens[i]+"_"+tokens[i+1]+"_"+tokens[i+2]+"_"+tokens[i+3]);
		}
		else if(code==10)
		{
			String[] tokens=attr.split(Parameters.splitstring);
			for(int i=0; i<tokens.length-5; i++)
				res.add(tokens[i]+"_"+tokens[i+1]+"_"+tokens[i+2]+"_"+tokens[i+3]+"_"+tokens[i+4]+"_"+tokens[i+5]);
		}
		else if(code>10&&code<=18){
			int p=code-11;
			String[] vals={"000","001","010","011","100","101","110","111"};
			String val=vals[p];
			boolean reverse= val.charAt(0)=='0' ? false : true;
			boolean mod= val.charAt(1)=='0' ? false : true;
			boolean four= val.charAt(2)=='0' ? false : true;
			String[] tokens=attr.split(Parameters.splitstring);
			for(int i=0; i<tokens.length; i++)
				res.add(BK.soundex(tokens[i],reverse, mod, four));
			
		}
		
		return res;
		
	}
	
	public String block(int index){
		HashSet<String> res=new HashSet<String>();
		for(int i=0; i<codes_DNF.get(index).size(); i++){
			mergeSets(res, blockHelp(codes_DNF.get(index).get(i),attributes_DNF.get(index).get(i)));
		}
		ArrayList<String> t=new ArrayList<String>(res);
		Collections.sort(t);
		String m="";
		for(int i=0; i<t.size(); i++)
			m+=(t.get(i)+" ");
		m=m.trim();
		if(m.equals(""))
			return null;
		else
			return m;
			
	}
	
	private void mergeSets(HashSet<String> a, HashSet<String> b){
		for(String c: b)
			a.add(c);
	}
	
	private boolean isInteger(String s) {
	    try { 
	        Integer.parseInt(s); 
	    } catch(NumberFormatException e) { 
	        return false; 
	    }
	    // only got here if we didn't return false
	    return true;
	}
	
	
}
