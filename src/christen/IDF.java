package christen;



import java.util.*;

public class IDF {
	
	TFIDF a;
	TFIDF b;
	public HashMap<String,Integer> IF;
	public int corpsize;

	public IDF(TFIDF a1, TFIDF a2){
		a=a1;
		b=a2;
		corpsize=a.corpussize+b.corpussize;
		calculateIF();
	}
	
	public void calculateIF(){
		IF=new HashMap<String,Integer>();
		for(String s: a.inverse_DF.keySet())
			if(b.inverse_DF.containsKey(s))
				IF.put(s,b.inverse_DF.get(s)+a.inverse_DF.get(s));
			else
				IF.put(s,a.inverse_DF.get(s));
		
		for(String s: b.inverse_DF.keySet())
			if(!IF.containsKey(s))
				IF.put(s, b.inverse_DF.get(s));
	}
}
