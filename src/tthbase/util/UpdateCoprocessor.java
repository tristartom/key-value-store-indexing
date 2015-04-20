package tthbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;

/**
1. update hbase coprocessor
2. disable physical deletion in all column families where indexed columns are in.
-KEEP_DELETED_CELLS = true
-TTL = HConstants.FOREVER
-maxVersion = Integer.MAX_VALUE
*/

public class UpdateCoprocessor {
    public static final byte[] FIXED_INDEX_CF = Bytes.toBytes("cf");
    public static final String USAGE = "Create a table and associated index;\n " +
            "Arguments:\n 1)zkserver 2)zkserver_port \n 3)table_name 4)list of cfs in table, in a single {},separated by ," +
            "5) INDEX_CP_NAME 6) INDEX_CP_PATH 7) INDEX_CP_CLASS "+
            "8)-[list of index columns in the format of cfName|colName]\n"+
            "format: if INDEX_CP_CLASS contains null, any coprocessor will be unloaded\n" +
            "***An example\n" +
            "saba20.watson.ibm.com 2181 weblog {cf} coprocessor\\$1 hdfs://saba20.watson.ibm.com:8020/index-coprocessor-0.1.0.jar org.apache.hadoop.hbase.coprocessor.index.SyncSecondaryIndexObserver cf\\|country cf\\|ip";
    
    public static String KEEP_DELETED_CELLS = "KEEP_DELETED_CELLS";// = "coprocessor$1";
    public static String INDEX_CP_NAME;// = "coprocessor$1";
    public static String INDEX_CP_PATH;// = "hdfs://saba20.watson.ibm.com:8020/index-coprocessor-0.1.0.jar";
    public static String INDEX_CP_CLASS;// = "org.apache.hadoop.hbase.coprocessor.index.SyncSecondaryIndexObserver";

    public static String zkserver;//saba20.watson.ibm.com
    public static String zkport;//2181
    public static String dataTableName;//weblog
    public static String cfList;
    public static String [] indexItems;//{cf|country, cf|ip}
    public static String cflist;
    public static String [] cfs;
    
    public static void main(String[] args) throws IOException {
        //six parameters to be given 
        int bar = 8;
        if(args.length<bar+1){
            System.out.println(USAGE);
            return;
        }
        
        zkserver = args[0];
        zkport = args[1];    
        dataTableName = args[2];
        cflist = args[3];
        INDEX_CP_NAME = args[4];
        INDEX_CP_PATH = args[5];
        INDEX_CP_CLASS = args[6];
        
        if(args.length>bar){
            indexItems = new String[args.length-bar];
            for(int i=bar;i<args.length;i++){
                indexItems[i-bar]= args[i];                
            }
        }
        
        cfs = getColFamilys(cflist);
        
        System.out.println("-----------Create table, deploy cp and create index definition-----");
        Configuration conf = HBaseConfiguration.create();        
        conf.set("hbase.zookeeper.quorum", zkserver);
        conf.set("hbase.zookeeper.property.clientPort",zkport);   
        System.out.println("TTDEBUG: update coprocessor to " + INDEX_CP_NAME + "=>" + INDEX_CP_CLASS);
        
        HTable dataTable = new HTable(conf, dataTableName);
        updateCoprocessor(conf, Bytes.toBytes(dataTableName));
    }
            
    private static void updateCoprocessor(Configuration conf, byte[] dataTableName) throws IOException{
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor desc = admin.getTableDescriptor(dataTableName);
        admin.disableTable(dataTableName);
        System.out.println("TTDEBUG: disable data table");
        if(INDEX_CP_CLASS.contains("null")) {
            desc.remove(Bytes.toBytes(INDEX_CP_NAME));
        } else {
            desc.setValue(INDEX_CP_NAME, INDEX_CP_PATH + "|" + INDEX_CP_CLASS + "|1001|arg1=1,arg2=2");
        }

        HColumnDescriptor descIndexCF = desc.getFamily(Bytes.toBytes("cf"));//TOREMOVE don't use cf, 
        //KEEP_DELETED_CELLS => 'true'
        descIndexCF.setKeepDeletedCells(true);
        descIndexCF.setTimeToLive(HConstants.FOREVER);
        descIndexCF.setMaxVersions(Integer.MAX_VALUE);

        admin.modifyTable(dataTableName, desc);
        System.out.println("TTDEBUG: modify data table");
        admin.enableTable(dataTableName);
        System.out.println("TTDEBUG: enable data table");
        HTableDescriptor descNew = admin.getTableDescriptor(dataTableName);
        //modify table is asynchronous, has to loop over to check
        while (!desc.equals(descNew)){
            System.out.println("TTDEBUG: waiting for descriptor to change: from " + descNew + " to " + desc);
            try {Thread.sleep(500);} catch(InterruptedException ex) {}
            descNew = admin.getTableDescriptor(dataTableName);
        }
    }

    private static String[] getColFamilys(String cflist) {
      //{cf1, cf2}
      String t = cflist.substring(cflist.indexOf('{')+1, cflist.lastIndexOf('}'));
      String temp[] = t.split(",");
      for(int i=0;i<temp.length;i++) temp[i] = temp[i].trim();
      return temp;
    }


    public static List<Pair<String,String>> getIndexCFAndColumn(HTableDescriptor htd) {        
        List<Pair<String,String>> result = new ArrayList<Pair<String,String>>();
        Pair<String,String> cfp = null;
        int i = 1;
        String index = null;
        do {
            index = htd.getValue(HIndexConstantsAndUtils.INDEX_INDICATOR + i);
            if (index != null) {
                String temp[] = index.split("\\"+HIndexConstantsAndUtils.INDEX_DELIMITOR);
                cfp = new Pair<String, String>(temp[0],temp[1]);
                if (cfp!=null) result.add(cfp);
            }
            i ++;
        } while (index != null);
        return result;
    }
}
