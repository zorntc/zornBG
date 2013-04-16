import java.util.*;

public class TablePartition {


 	// static String[] arguments = {"workloadCnt", "actionName", "readOnly", "column", "table"};
	final static int[] workloadCnt = {8,7,100,1,1,
		9,1,1,1,1,
		1,1};
	final static String[] actionName = {"AcceptFriend", "AcceptFriend", "AcceptFriend", "AcceptFriend", "AcceptFriend",
		"AcceptFriend", "AcceptFriend", "AcceptFriend", "AcceptFriend", "AcceptFriend",
		"ViewProfile", "ViewProfile"};
	final static boolean[] readOnly = {false, false, false, false, false, 
		false, false, false, false, false,  
		false, false};
	final static String[] column = {"inviteeID","inviteeID","inviterID","inviterID","inviteeID",
		"inviteeID","inviteeID","inviterID","inviteeID","inviteeID",
		"userID","userID"};
	final static String[] table = {"pendfriendship", "pendfriendship", "pendfriendship", "pendfriendship", "conffriendship",
		"conffriendship", "conffriendship", "conffriendship", "conffriendship", "conffriendship",
		"User", "User"};

/*
 	Previous test for ROUTING ATTRIBUTE; 11am, Apr 16 
	final static int[] workloadCnt = {1,1,1,1,1,1};
	final static String[] actionName = {"AcceptFriend", "AcceptFriend", "AcceptFriend", "AcceptFriend", "AcceptFriend", "AcceptFriend"};
	final static boolean[] readOnly = {false,false,false,false,false,false};
	final static String[] column = {"inviteeID","inviterID","inviterID","inviteeID","inviteeID","inviterID"};
	final static String[] table = {"pendfriendship", "pendfriendship", "conffriendship", "conffriendship", "conffriendship",
			"conffriendship"};
*/
	private static LinkedList<Table> partitionList = new LinkedList<Table>();
	private static LinkedList<Table> replicationList = new LinkedList<Table>();
	private static LinkedList<Procedure> routAtrrList = new LinkedList<Procedure>(); 

	static LinkedList<Table> getPartitionList(){
		return partitionList;
	}

	public static String isArgLengthEqual(){
		return (workloadCnt.length != actionName.length)? "workloadCnt or actionName" : 
			(workloadCnt.length != readOnly.length)? "readOnly" :
			(workloadCnt.length != column.length)? "column" :
			(workloadCnt.length != table.length)? "table" : null;
	}

	private static void partition(){
		int i;
		Table ta = null;
		for(i = 0; i < column.length; i++){
			ta = null;
			for(Table t : partitionList){
				if(!t.name.equals(table[i]))
					continue;
				ta = t;
				break;
			}
			if(ta == null){						 
				partitionList.add(new Table(table[i]));
				ta = partitionList.getLast();
			}
			ta.inc(column[i], workloadCnt[i]);
			if(!readOnly[i]){
				ta.replication = false;
				ta.modifiedCol.add(column[i]);
			}
		}
	}

	private static void replication(){
		ListIterator<Table> ite = partitionList.listIterator(0);
		Table t;
		while(ite.hasNext()){
			t = ite.next();
			if(t.replication)
				replicationList.add(t);
		}

		ite = replicationList.listIterator(0);
		while(ite.hasNext()){
			t = ite.next();
			partitionList.remove(t);
		}
	}

	private static void routingAttr(){
		int i;
		Procedure pr = null;
		for(i = 0; i < column.length; i++){
			pr = null;
			for(Procedure p : routAtrrList){
				if(!p.name.equals(actionName[i]))
					continue;
				pr = p;
				break;
			}
			if(pr == null){						 
				routAtrrList.add(new Procedure(actionName[i]));
				pr = routAtrrList.getLast();
			}
			pr.addAttr(column[i], table[i]);
		}		
	}

	private static void printlog(){
		System.out.println("==PARTITION & SECONDARY INDEX==");
		for(Table tl: partitionList){
			System.out.printf("partition %s by %s, second index: %s\n", tl.name, tl.getPartAttr(), tl.getSecIndex());
		}
		System.out.println("==REPLICATION==");
		for(Table tl: replicationList){
			System.out.printf("replicate %s\n", tl);
		}
		System.out.println("==ROUTING ATTRIBUTE==");
		for(Procedure p: routAtrrList){
			System.out.printf("%s's routing attribute is %s\n", p.name, p.getRoutAtrr());
		}
	}
	
	public static void main(String[] args){
		if(isArgLengthEqual() != null){
			System.err.println("arrays length is inconsistent: " + isArgLengthEqual());
			System.exit(1);
		}

		partition();	// is not finished without calling replication()
		replication();
		routingAttr();		

		printlog();
	}
}

class Table{
	String name = null;
	private String partitionAttr = null;
	private String secIndex = null;
	boolean replication = true;
	TreeMap<String, Integer> attr = new TreeMap<String, Integer>();		// counting attr frequency
	HashSet<String> modifiedCol = new HashSet<String>();		// what attr is not read-only 

	public boolean equals(String s){
		return (s.equalsIgnoreCase(name));
	}

	public Table(String s){
		name = s;
	}

	public void inc(String s, int cnt){
		if(attr.containsKey(s)){
			int val = attr.remove(s);
			attr.put(s, val + cnt);
		}
		else{
			attr.put(s, cnt);
		}
	}

	public void computePartAttrSecIndex(){
		Map.Entry<String, Integer> me;
		int max = 0, maxMax = 0;
		while((me = attr.pollFirstEntry())!= null){
			if(me.getValue() < max)
				continue;
			if(maxMax <= me.getValue()){
				if(!modifiedCol.contains(partitionAttr)){
					max = maxMax;
					secIndex = partitionAttr;
				}
				maxMax = me.getValue();
				partitionAttr = me.getKey();
			}
			else{ 	// max <= me < maxMax
				if(!modifiedCol.contains(me.getKey()))
					continue;
				max = me.getValue();
				secIndex = me.getKey();
			}
		}
		attr = null;	// free attr TreeMap
		if(partitionAttr == null)
			partitionAttr = "NIL";
		if(secIndex == null)
			secIndex = "NIL";
	}

	public String getPartAttr(){
		if(partitionAttr == null)
			computePartAttrSecIndex();
		return partitionAttr;
	}

	public String getSecIndex(){
		if(secIndex == null)
			computePartAttrSecIndex();
		return secIndex;
	}
	
	public String toString(){
		return name;
	}
}

class Procedure{
	String name;
	String routAtrr;
	String routTable;		// TODO havn't set yet. null all the time right now
	TreeMap<String, Integer> attr = new TreeMap<String, Integer>();
	LinkedList<String> attrCdt = new LinkedList<String>();		// attribute candidate 

	public Procedure(String sname){
		name = sname;
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
		for(Table t : TablePartition.getPartitionList()){
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
			routAtrr = "NIL";
		attr = null;	// free attr TreeMap
		return routAtrr;
	}

}
