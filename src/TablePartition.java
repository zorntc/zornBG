import java.util.*;

public class TablePartition {

	static int[] workloadCnt = {8,7,100,1,1,9,1,1,1,1,1,1};
	static String[] actionName = {"AcceptFriend", "AcceptFriend", "AcceptFriend", "AcceptFriend", "AcceptFriend", "AcceptFriend", "AcceptFriend", "AcceptFriend", "AcceptFriend", "AcceptFriend"};
	static boolean[] readOnly = {true, true, false, true, false, true, true, true, true, false, true, true};
	static String[] column = {"inviteeID","inviteeID","inviterID","inviterID","inviteeID","inviteeID","inviteeID","inviterID","inviteeID","inviteeID","userID","userID"};
	static String[] table = {"pendfriendship", "pendfriendship", "pendfriendship", "pendfriendship", "conffriendship", "conffriendship", "conffriendship", "conffriendship", "conffriendship", "conffriendship", "User", "User"};

	static LinkedList<Table> partitionList = new LinkedList<Table>();
	static LinkedList<Table> replicationList = new LinkedList<Table>();

	static void partition(){
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

	static void replication(){
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

	public static void main(String[] args){
		partition();
		replication();

		for(Table tl: partitionList){
			System.out.printf("partition %s by %s, second index: %s\n", tl.name, tl.getPartAttr(), tl.getSecIndex());
		}
		for(Table tl: replicationList){
			System.out.printf("replicate %s\n", tl.name);
		}
	}
}

class Table{
	String name = null;
	private String partitionAttr = null;
	private String secIndex = null;
	boolean replication = true;
	TreeMap<String, Integer> attr = new TreeMap<String, Integer>();
	HashSet<String> modifiedCol = new HashSet<String>();

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
}
