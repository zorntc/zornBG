import java.io.File;
import java.util.ArrayList;

import org.w3c.dom.*;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;


/**
 * @author Arpit
 */
public class HorticultureFinalProject{
   
    static SchemaExtractor[] schemaExtractor= new SchemaExtractor[4];
    static ProceedureExtractor[] proceedureExtractor= new ProceedureExtractor[11];
    static WorkloadExtractor[] workloadExtractor= new WorkloadExtractor[11];
   
   
    public static void setworkload()
    {
       
        int counter=0;
        String Name,attributes,backup_of_s;
       
        try {
            DocumentBuilderFactory odbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder odb =  odbf.newDocumentBuilder();
            Document odoc = odb.parse (new File("workload.xml"));
            odoc.getDocumentElement ().normalize ();
           // System.out.println ("Root element of the doc is " + odoc.getDocumentElement().getNodeName());
            NodeList LOP = odoc.getElementsByTagName("Action");
            int totalPersons =LOP.getLength();
            //System.out.println("Total no of Tables : " + totalPersons);
            for(int s=0; s<LOP.getLength() ; s++)
        {

                Node FPN =LOP.item(s);
                if(FPN.getNodeType() == Node.ELEMENT_NODE)
          {
                    Element firstPElement = (Element)FPN;
                  
                    NodeList oNameList = firstPElement.getElementsByTagName("Name");
                    Element firstNameElement = (Element)oNameList.item(0);
                    NodeList textNList = firstNameElement.getChildNodes();
                   
                   
                   
                    backup_of_s = new String(((Node)textNList.item(0)).getNodeValue().trim().toString());
                   
                   
                    //System.out.println("Backupofs"+backup_of_s);
                    workloadExtractor[s]=new WorkloadExtractor();
                   
                    workloadExtractor[s].setAction(backup_of_s);
                   
                   
                  
                    NodeList IDList = firstPElement.getElementsByTagName("frequency");
                    Element IDElement = (Element)IDList.item(0);
                    NodeList textIDList = IDElement.getChildNodes();
                  
                   
                    backup_of_s = new String(((Node)textIDList.item(0)).getNodeValue().trim());
                   
                    //System.out.println("Backupofs"+backup_of_s);
                    workloadExtractor[s].setFrequency(backup_of_s);
                   
                   
                           
                }    //end of if clause
            }        //end of for loop with variable s
        }catch (SAXParseException err) {
        System.out.println (err.getMessage ());
        }catch (SAXException e) {
             e.printStackTrace ();
        }catch (Throwable t) {
        t.printStackTrace ();
        }
 

       
    }
   
    public static void setProceedure()
    {
        int counter=0;
        String Name,attributes,backup_of_s;
       
        try {
            DocumentBuilderFactory odbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder odb =  odbf.newDocumentBuilder();
            Document odoc = odb.parse (new File("Proceedure.xml"));
            odoc.getDocumentElement ().normalize ();
           // System.out.println ("Root element of the doc is " + odoc.getDocumentElement().getNodeName());
            NodeList LOP = odoc.getElementsByTagName("Query");
            int totalPersons =LOP.getLength();
            //System.out.println("Total no of Tables : " + totalPersons);
            for(int s=0; s<LOP.getLength() ; s++)
        {

                Node FPN =LOP.item(s);
                if(FPN.getNodeType() == Node.ELEMENT_NODE)
          {
                    Element firstPElement = (Element)FPN;
                  
                    NodeList oNameList = firstPElement.getElementsByTagName("syntax");
                    Element firstNameElement = (Element)oNameList.item(0);
                    NodeList textNList = firstNameElement.getChildNodes();
                   
                   
                   
                    backup_of_s = new String(((Node)textNList.item(0)).getNodeValue().trim().toString());
                    //System.out.println(backup_of_s);
                   
                    proceedureExtractor[s]=new ProceedureExtractor();
                    proceedureExtractor[s].setQuery(backup_of_s);
                   
                   
                           
                }    //end of if clause
            }        //end of for loop with variable s
        }catch (SAXParseException err) {
        System.out.println (err.getMessage ());
        }catch (SAXException e) {
             e.printStackTrace ();
        }catch (Throwable t) {
        t.printStackTrace ();
        }
 
    }
   

   
    public static void setSchema()
    {
        int counter=0;
        String Name,attributes,backup_of_s;
       
        try {
            DocumentBuilderFactory odbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder odb =  odbf.newDocumentBuilder();
            Document odoc = odb.parse (new File("BGSchema.xml"));
            odoc.getDocumentElement ().normalize ();
           // System.out.println ("Root element of the doc is " + odoc.getDocumentElement().getNodeName());
            NodeList LOP = odoc.getElementsByTagName("Table");
            int totalPersons =LOP.getLength();
            //System.out.println("Total no of Tables : " + totalPersons);
            for(int s=0; s<LOP.getLength() ; s++)
        {

                Node FPN =LOP.item(s);
                if(FPN.getNodeType() == Node.ELEMENT_NODE)
          {
                    Element firstPElement = (Element)FPN;
                  
                    NodeList oNameList = firstPElement.getElementsByTagName("Name");
                    Element firstNameElement = (Element)oNameList.item(0);
                    NodeList textNList = firstNameElement.getChildNodes();
                   
                   
                   
                    backup_of_s = new String(((Node)textNList.item(0)).getNodeValue().trim().toString());
                   
                   
                    schemaExtractor[s]=new SchemaExtractor();
                    schemaExtractor[s].setTableName(backup_of_s);
                   
                   
                  
                    NodeList IDList = firstPElement.getElementsByTagName("Attributes");
                    Element IDElement = (Element)IDList.item(0);
                    NodeList textIDList = IDElement.getChildNodes();
                  
                   
                    backup_of_s = new String(((Node)textIDList.item(0)).getNodeValue().trim());
                  
                    schemaExtractor[s].setAttributeNames(backup_of_s);
                   
                           
                }    //end of if clause
            }        //end of for loop with variable s
        }catch (SAXParseException err) {
        System.out.println (err.getMessage ());
        }catch (SAXException e) {
             e.printStackTrace ();
        }catch (Throwable t) {
        t.printStackTrace ();
        }
 
    }
   
   
    static ArrayList AttributeNames = new ArrayList();
    static ArrayList TableNames = new ArrayList();
    static ArrayList QueryNames = new ArrayList();
   
   
    public static void printIndexes(String string, char ch,ArrayList k) {
        int index = 0;
        while((index = string.indexOf(ch, index)) != -1) {
            index = string.indexOf(ch, index);
            //System.out.println("index"+index);
            k.add(index);
            index++;
        }
    }
   
    public static void computePartition(String Query)
    {
        ArrayList storingDotPosition = new ArrayList();
        ArrayList storingQuestionMarkPosition = new ArrayList();
        ArrayList storingspacePosition=new ArrayList();
        ArrayList storingTableposition=new ArrayList();
       
        System.out.println("*******************Proceedure Partition Analyzation*******************************");
        System.out.println("Input Query::-"+Query);
        //Select u.* from User u, Friendship f where f.inviteeId=? and f.ReqId=u.userid and    f.status=confirmed
       
        String[] splits = Query.split("where");
        System.out.println("splits.size: " + splits.length);
       
        System.out.println("Line"+splits[0]);
        System.out.println("Line"+splits[1]);
       
        String[] splits1 = splits[0].split("from");
       
        printIndexes(splits[1], '.',storingDotPosition);
        printIndexes(splits[1], '?',storingQuestionMarkPosition);
        printIndexes(splits[0], ' ',storingspacePosition);
       
       
       
       
        System.out.println("****************Dot Position Stored Display Logic*************");
        for(int i=0;i<storingDotPosition.size();i++)
        {
            System.out.println("Values"+storingDotPosition.get(i));
        }
        System.out.println("****************Dot Position Stored Display Logic*************");
       
       
        System.out.println("****************Question Mark Position Stored Display Logic*************");
        for(int i=0;i<storingQuestionMarkPosition.size();i++)
        {
            System.out.println("Values"+storingQuestionMarkPosition.get(i));
        }
        System.out.println("****************Question Mark Position Stored Display Logic*************");
           
        System.out.println("****************Printing values between . and ? Putting Attribute Names and Table Names*************");
        String result="";
        String QueryName="";
        int start=0;
        int end=0;
        int storeIndex=0;
       
        for(int i=0;i<storingQuestionMarkPosition.size();i++)
        {
            //start=
            result=splits[1].substring(Integer.parseInt(storingDotPosition.get(i).toString())+1,Integer.parseInt(storingQuestionMarkPosition.get(i).toString())-1);
            AttributeNames.add(result);
            System.out.println("Attribute"+AttributeNames.get(i).toString());
            result=splits[1].substring(Integer.parseInt(storingDotPosition.get(i).toString())-1,Integer.parseInt(storingDotPosition.get(i).toString()));
            System.out.println("Result"+result);
            //store=result[0];
            System.out.println("spilits[1]"+splits1[1]);
            printIndexes(splits1[1],result.charAt(0),storingTableposition);
            System.out.println("Table Position Values:-"+storingTableposition.get(i));
            for(int k=Integer.parseInt(storingTableposition.get(i).toString())-2;k>=0;k--)
            {
                if(splits1[1].charAt(k)==' ')
                {
                    storeIndex=k+1;
                    break;
                }
            }
           
            QueryName=splits[0].substring(0,Integer.parseInt(storingspacePosition.get(0).toString()));
            QueryNames.add(QueryName);
            System.out.println("Query"+QueryNames.get(i).toString());
            TableNames.add(splits1[1].substring(storeIndex,Integer.parseInt(storingTableposition.get(i).toString())-1));
            System.out.println("Table"+TableNames.get(i).toString());
           
       
        }
        for(int i=0;i<AttributeNames.size();i++)
        {
            System.out.println("splits1"+splits1[1]);
            System.out.println("Result:-"+result);
            System.out.println("Attribute Name Values:-"+AttributeNames.get(i));
            System.out.println("Table Name Values:-"+TableNames.get(i));
        }
        for(int i=0;i<QueryNames.size();i++)
        {
            //System.out.println("Result:-"+result);
            System.out.println("Query Name Values:-"+QueryNames.get(i));
        }
       
        System.out.println("****************Printing values between . and ? Putting Attribute Names and Table Names*************");
       
       
       
        System.out.println("*******************Proceedure Partition Analyzation*******************************");
    }
   
   
    public static void main (String argv []){
       
        setSchema();
       
        for(int x=0;x<(schemaExtractor.length);x++)
        {
            System.out.println("Table : " + schemaExtractor[x].getTableName());   
            System.out.println("Attribute : " + schemaExtractor[x].getAttributeNames());
        }
       
        setProceedure();
       
        for(int y=0;y<(proceedureExtractor.length);y++)
        {
        System.out.println("Queries : " + proceedureExtractor[y].getQuery());   
        }
       
        setworkload();
       
        for(int z=0;z<(workloadExtractor.length);z++)
        {
        System.out.println("Action : " + workloadExtractor[z].getAction());   
        System.out.println("Frequency : " + workloadExtractor[z].getFrequency());   
       
        }
        for(int t=0;t<proceedureExtractor.length;t++)
        {
        computePartition(proceedureExtractor[t].getQuery());
        }
       
        System.out.println("Final Output");
       
        for(int i=0;i<AttributeNames.size();i++)
        {
            //System.out.println("splits1"+splits1[1]);
            //System.out.println("Result:-"+result);
            System.out.println("Attribute Name Values:-"+AttributeNames.get(i));
            System.out.println("Table Name Values:-"+TableNames.get(i));
            System.out.println("Query Name Values:-"+QueryNames.get(i));
            //System.out.println("Table Name Values:-"+TableNames.get(i));
        }
       
        for(int i=0;i<TableNames.size();i++)
        {
            //System.out.println("splits1"+splits1[1]);
            //System.out.println("Result:-"+result);
            //System.out.println("Attribute Name Values:-"+AttributeNames.get(i));
            //System.out.println("Table Name Values:-"+TableNames.get(i));
        }
        for(int i=0;i<QueryNames.size();i++)
        {
            //System.out.println("Result:-"+result);
           
        }
        System.out.println("Final Output");
       
       
       }
}