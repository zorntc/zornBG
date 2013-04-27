import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

class Table implements Comparable<Table>{	// compare tables base on temperature
	String name = null;
	private String partitionAttr = null;
	private LinkedList<String> secIndex = new LinkedList<String>();
	boolean replication = true;
//	private double tableSize;		// for temperature
	private int numTxn;				// for temperature
	TreeMap<String, Integer> attr = new TreeMap<String, Integer>();		// counting attr frequency
	HashSet<String> modifiedCol = new HashSet<String>();		// what attr is not read-only 

	public Table(String s){
		name = s;
	}

	public void setTableSize(double s){
//		tableSize = s; 
	}
	
	public void setNumTxn(int p){
		numTxn = p;
	}
	
	public void incNumTxn(int p){
		numTxn += p;
	}
	
	public void inc(String s, int cnt){
		s = s.toLowerCase();
		if(attr.containsKey(s)){
			int val = attr.remove(s);
			attr.put(s, val + cnt);
		}
		else{
			attr.put(s, cnt);
		}
		incNumTxn(cnt);
	}

	public void computePartAttrSecIndex(){
		Map.Entry<String, Integer> me;
		int max = 0, maxMax = 0;
		while((me = attr.pollFirstEntry())!= null){
			if(me.getValue() < max)
				continue;
			if(maxMax <= me.getValue()){
				if(!modifiedCol.contains(partitionAttr) && partitionAttr != null){	// partitionAttr is read-only, throw partitionAttr to secIndex
					max = maxMax;
					secIndex.add(partitionAttr);
				}
				maxMax = me.getValue();
				partitionAttr = me.getKey();
			}
			else{ 	// max <= me < maxMax
				if(!modifiedCol.contains(me.getKey()))
					continue;
				max = me.getValue();
				secIndex.add(me.getKey());
			}
		}
		attr = null;	// free attr TreeMap
		if(partitionAttr == null)
			partitionAttr = "NIL";
		if(secIndex.size() == 0)
			secIndex.add("NIL");
	}

	public String getPartAttr(){
		if(partitionAttr == null)
			computePartAttrSecIndex();
		return partitionAttr;
	}

	public LinkedList<String> getSecIndex(){
		if(secIndex.size() == 0)
			computePartAttrSecIndex();
		return secIndex;
	}

	public boolean equals(String s){
		return (s.equalsIgnoreCase(name));
	}
	
	public String toString(){
		return name;
	}
	
	public int compareTo(Table t){	
		/* compare tables base on temperature
		 double ret = (double) tableSize / (double) numTxn - (double) t.tableSize / (double) t.numTxn;
		*/		
		
		// compare tables base on numTxn
		double ret = (double) numTxn - (double) t.numTxn;
		return (ret > 0)? 1 :
			(ret < 0)? -1 : 0; 
	}
}
