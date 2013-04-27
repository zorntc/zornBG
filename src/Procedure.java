import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;


class Procedure{
	String name; 
	String routAtrr;
	String routTable;		// TODO havn't set yet. null all the time right now
	TreeMap<String, Integer> attr = new TreeMap<String, Integer>();
	LinkedList<String> attrCdt = new LinkedList<String>();		// attribute candidate 
	Design root;

	public Procedure(String sname, Design d){
		name = sname;
		root = d;
	}

	public Procedure(String sname, String ra, String rt){
		name = sname;
		routAtrr = ra;
		routTable = rt;
	}

	public boolean addAttr(String col, String table){
		String s = col + " @ " + table;
		if(attr.containsKey(s)){
			int val = attr.remove(s);
			attr.put(s, val + 1);
		}
		else{
			attr.put(s, 1);
		}
		return attrCdt.add(s);
	}

	// a valid routing attribute is a partition attribute of a table
	boolean isValidRoutAttr(String s){
		for(Table t : root.getPartitionList()){
			if(s.equalsIgnoreCase(t.getPartAttr() + " @ " + t.name))
				return true;
		}
		return false;
	}

	public String getRoutAtrr(){
		if(routAtrr != null)
			return routAtrr;
		Map.Entry<String, Integer> me;
		int cnt = 0;
		while((me = attr.pollFirstEntry())!= null){
			if(me.getValue() <= cnt || !isValidRoutAttr(me.getKey()))
				continue;
			cnt = me.getValue();
			routAtrr = me.getKey();
		}
		if(routAtrr == null)
			routAtrr = attrCdt.getFirst() + " [Not a valid routing parameter]";
		attr = null;	// free attr TreeMap
		return routAtrr;
	}

}