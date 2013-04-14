import java.util.*;

public class TablePartition {

	static int[] workloadCnt = {8,7,1,1,1,9,1,1,1,1};
	static String[] actionName = {"AcceptFriend", "AcceptFriend", "AcceptFriend", "AcceptFriend", "AcceptFriend", "AcceptFriend", "AcceptFriend", "AcceptFriend", "AcceptFriend", "AcceptFriend"};
	static boolean[] readOnly = {true, true, true, false, true, true, true, false, false, true};
	static String[] whereClause = {"inviteeID","inviterID","inviteeID","inviteeID","reqID","reqID","reqID","reqID","1","1"};
	static String[] table = {"pendfriendship", "pendfriendship", "friendship", "friendship", "friendship", "friendship", "friendship", "friendship", "qwer", "qwer"};

	static LinkedList<Table> tableList = new LinkedList<Table>();

	static void partition(){
		int i;
		Table ta = null;
		for(i = 0; i < whereClause.length; i++){
			ta = null;
			for(Table t : tableList){
				if(!t.name.equals(table[i]))
					continue;
				ta = t;
				break;
			}
			if(ta == null){						 
				tableList.add(new Table(table[i]));
				ta = tableList.getLast();
			}
			ta.inc(whereClause[i], workloadCnt[i]);
		}
	}

	public static void main(String[] args){
		partition();
		for(Table tl: tableList){
			System.out.printf("%s: partition attr is %s\n", tl.name, tl.getPartAttr());
		}
	}
}

class Table{
	String name = null;
	String partitionAttr = null;
	TreeMap<String, Integer> attr = new TreeMap<String, Integer>();

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

	public String getPartAttr(){
		if(partitionAttr != null) 
			return partitionAttr;
		Map.Entry<String, Integer> me;
		int cnt = 0;
		while((me = attr.pollFirstEntry())!= null){
			if(me.getValue() <= cnt)
				continue;
			cnt = me.getValue();
			partitionAttr = me.getKey();
		}
		return partitionAttr;
	}
}
