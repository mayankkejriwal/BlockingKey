package christen;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class BK {
	
	
	
	public static int[] ExactMatch(String[] arr1, String[] arr2){
		int[] weight=new int[arr1.length];
		for(int i=0; i<arr1.length; i++){
			if(arr1[i].equals(arr2[i]))
				weight[i]=1;
			else
				weight[i]=0;
		}
		return weight;
	}
	private static boolean containscommon(String[] tokens1, String[] tokens2){
		
		for(int i=0; i<tokens1.length; i++){
			for(int j=0; j<tokens2.length; j++)
				if(tokens1[i].equals(tokens2[j]))
					return true;
		}
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
	
	private static boolean containscommonInteger(String[] tokens1, String[] tokens2){
		
		for(int i=0; i<tokens1.length; i++){
			if(!isInteger(tokens1[i]))
				continue;
			for(int j=0; j<tokens2.length; j++)
				if(isInteger(tokens2[j])&&(int)Integer.parseInt(tokens1[i])==(int)Integer.parseInt(tokens2[j]))
					{
					return true;
					}
		}
		return false;
			
	}
	
	private static boolean checkCondition(int a, int b){
		if(a==b||a-1==b||a+1==b)
			return true;
		
		return false;
	}
	private static boolean containsSameOffByOneInteger(String[] tokens1, String[] tokens2){
		
		for(int i=0; i<tokens1.length; i++){
			if(!isInteger(tokens1[i]))
				continue;
			for(int j=0; j<tokens2.length; j++)
				if(isInteger(tokens2[j])&&checkCondition(Integer.parseInt(tokens1[i]),Integer.parseInt(tokens2[j])))
					return true;
		}
		return false;
			
	}
	
	
	
	public static int[] CommonToken(String[] arr1, String[] arr2){
		if(arr1.length!=arr2.length)
			return null;
		int[] weight=new int[arr1.length];
		for(int i=0; i<arr1.length; i++){
			String[] tokens1=arr1[i].split(Parameters.splitstring);
			String[] tokens2=arr2[i].split(Parameters.splitstring);
			if(containscommon(tokens1,tokens2))
				weight[i]=1;
			else
				weight[i]=0;
		}
		return weight;
	}
	
	public static int[] CommonInteger(String[] arr1, String[] arr2){
		if(arr1.length!=arr2.length)
			return null;
		int[] weight=new int[arr1.length];
		for(int i=0; i<arr1.length; i++){
			String[] tokens1=arr1[i].split(Parameters.splitstring);
			String[] tokens2=arr2[i].split(Parameters.splitstring);
			if(containscommonInteger(tokens1,tokens2))
				weight[i]=1;
			else
				weight[i]=0;
		}
		return weight;
	}
	
	public static int[] CommonOrOffByOneInteger(String[] arr1, String[] arr2){
		if(arr1.length!=arr2.length)
			return null;
		int[] weight=new int[arr1.length];
		if(arr1.length!=arr2.length)
			return null;
		for(int i=0; i<arr1.length; i++){
			String[] tokens1=arr1[i].split(Parameters.splitstring);
			String[] tokens2=arr2[i].split(Parameters.splitstring);
			if(containsSameOffByOneInteger(tokens1,tokens2))
				weight[i]=1;
			else
				weight[i]=0;
		}
		return weight;
	}
	
	private static boolean containsCommonNFirst(String[] tokens1, String[] tokens2, int n){
		for(int i=0; i<tokens1.length; i++){
			if(n>tokens1[i].length())
				continue;
			String temp1=tokens1[i].substring(0,n);
			for(int j=0; j<tokens2.length; j++){
				if(n>tokens2[j].length())
					continue;
				if(temp1.equals(tokens2[j].substring(0,n)))
					return true;
			}
		}
		return false;
			
	}
	
	public static int[] CommonNFirst(String[] arr1, String[] arr2, int n){
		if(arr1.length!=arr2.length)
			return null;
		int[] weight=new int[arr1.length];
		
		for(int i=0; i<arr1.length; i++){
			String[] tokens1=arr1[i].split(Parameters.splitstring);
			String[] tokens2=arr2[i].split(Parameters.splitstring);
			if(containsCommonNFirst(tokens1,tokens2, n))
				weight[i]=1;
			else
				weight[i]=0;
		}
		return weight;
	}
	private static boolean ArraysEqual(String[] tokens1, String[] tokens2, int i, int j, int n){
		int count=0;
		for(int k1=i; k1<tokens1.length; k1++){
			if(j>=tokens2.length)
				break;
			if(tokens1[k1].equals(tokens2[j])){
					count++;
					j++;}
			else {
					if(count>=n)
						return true;
					else
						return false;
				}
		}
		if(count>=n)
			return true;
		else
			return false;
	}
	private static boolean containsCommonTokenNGram(String[] tokens1, String[] tokens2, int n){
		if(n<1)
			return false;
		for(int i=0; i<=tokens1.length-n; i++){
			for(int j=0; j<=tokens2.length-n; j++){
				if(tokens1[i].equals(tokens2[j]))
					if(ArraysEqual(tokens1,tokens2,i,j,n))
						return true;
			}
		}
		return false;
			
	}
	
	public static int[] CommonTokenNGram(String[] arr1, String[] arr2, int n){
		int[] weight=new int[arr1.length];
		if(arr1.length!=arr2.length)
			return null;
		for(int i=0; i<arr1.length; i++){
			String[] tokens1=arr1[i].split(Parameters.splitstring);
			String[] tokens2=arr2[i].split(Parameters.splitstring);
			if(containsCommonTokenNGram(tokens1,tokens2, n))
				weight[i]=1;
			else
				weight[i]=0;
		}
		return weight;
	}
	
	public static int[] tokenTFIDF(ArrayList<HashMap<String,Double>> w1,ArrayList<HashMap<String,Double>> w2, double delta){
		int[] weight=new int[w1.size()];
		if(w1.size()!=w2.size())
			return null;
		for(int i=0; i<weight.length; i++){
			double sum=0.0;
			for(String p:w1.get(i).keySet())
				if(w2.get(i).containsKey(p))
					sum+=(w1.get(i).get(p)*w2.get(i).get(p));
			if(sum>=delta)
				weight[i]=1;
			else
				weight[i]=0;
		}
		return weight;
	}
	
	private static String rev(String orig){
		String res="";
		for(int i=orig.length()-1; i>=0; i--)
			res+=orig.charAt(i);
		return res;
	}
	
	//string length assumed to be less than 4
	private static String padWithZeros(String orig){
		String res=new String(orig);
		for(int i=orig.length(); i<=3; i++)
			res+="0";
		return res;
	}
	
	//return soundex encoding of orig. orig can be anything, so we're taking liberty with soundex def.
	public static String soundex(String orig, boolean reverse, boolean mod, boolean four){
		String alphabet="abcdefghijklmnopqrstuvwxyz";
		String translation_nonmod="01230120022455012623010202";
		String translation_mod="01360240043788015936020505";
		String translation= mod ? translation_mod : translation_nonmod;
		if(orig.length()==0)
			return "";
		
		String src= reverse ?  rev(orig): (new String(orig))  ;
		HashSet<Character> charset=new HashSet<Character>();
		String res=""+src.charAt(0);
		for(int i=1; i<src.length(); i++){
			int index=alphabet.indexOf(src.charAt(i));
			if(index==-1)
				continue;
			char conv=translation.charAt(index);
			if(conv=='0')
				continue;
			if(charset.contains(conv))
					continue;
			else{
				charset.add(conv);
				res+=conv;
				
			}
		}
		
		
		
		if(res.length()==4||(!four))
			return res;
		
		if(res.length()>4)
			return res.substring(0,4);
		else
			return padWithZeros(res);
		
	}
	
	private static boolean containsCommonSoundex(String[] tokens1, String[] tokens2, boolean reverse, boolean mod, boolean four){
		for(int i=0; i<tokens1.length; i++)
			for(int j=0; j<tokens2.length; j++){
				if(soundex(tokens1[i],reverse,mod,four).equals(soundex(tokens2[j], reverse, mod, four))&&!soundex(tokens2[j], reverse, mod, four).equals(""))
					return true;
			}
		return false;
	}
	
	public static int[] soundex(String[] arr1, String[] arr2, boolean reverse, boolean mod, boolean four){
		if(arr1.length!=arr2.length)
			return null;
		int[] weight=new int[arr1.length];
		
		for(int i=0; i<arr1.length; i++){
			String[] tokens1=arr1[i].split(Parameters.splitstring);
			String[] tokens2=arr2[i].split(Parameters.splitstring);
			if(containsCommonSoundex(tokens1,tokens2, reverse, mod, four))
				weight[i]=1;
			else
				weight[i]=0;
		}
		return weight;
	}
	
	public static void main(String[] args){
		
		System.out.println(soundex("damian", false, false, true));
	}

}
