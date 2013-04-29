import java.util.LinkedList;

/**
 * @author Arpit
 */
public class SchemaExtractor 
{

	String tableName;
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	String attributeNames;
	public String getAttributeNames() {
		return attributeNames;
	}
	public void setAttributeNames(String attributeNames) {
		this.attributeNames = attributeNames;
	}
	
	/* Zorn added start */
	private LinkedList<String> everyAttribute = new LinkedList<String>(); // need 
	public LinkedList<String> getEveryAttribute() {
		everyAttribute.add("userid");
		return everyAttribute;
	}
	
	private int txnCnt = 0;
	public int getTxnCnt(){
		return txnCnt;
	}
	/* Zorn added end */
}
