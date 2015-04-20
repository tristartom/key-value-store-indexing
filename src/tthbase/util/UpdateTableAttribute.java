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

import tthbase.util.HIndexConstantsAndUtils;

import org.apache.hadoop.hbase.HConstants;

/**
1. update hbase coprocessor
2. disable physical deletion in all column families where indexed columns are in.
-KEEP_DELETED_CELLS = true
-TTL = HConstants.FOREVER
-maxVersion = Integer.MAX_VALUE
*/

public class UpdateTableAttribute {
    public static final String INDEX_INDICATOR = "secondaryIndex$";
    public static final String INDEX_DELIMITOR = "|";
    public static final String TABLE_NAME_DELIMITOR = "_";
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
    public static String cf;
    
    public static void main(String[] args) throws IOException {
        //six parameters to be given 
        int bar = 3;
        if(args.length<bar+1){
            System.out.println(args.length);
            return;
        }
        
        zkserver = args[0];
        zkport = args[1];    
        dataTableName = args[2];
        cf = args[3];
        
        if(args.length>bar){
            indexItems = new String[args.length-bar];
            for(int i=bar;i<args.length;i++){
                indexItems[i-bar]= args[i];                
            }
        }
        
        System.out.println("-----------Create table, deploy cp and create index definition-----");
        Configuration conf = HBaseConfiguration.create();        
        conf.set("hbase.zookeeper.quorum", zkserver);
        conf.set("hbase.zookeeper.property.clientPort",zkport);   
        System.out.println("TTDEBUG: update coprocessor to " + INDEX_CP_NAME + "=>" + INDEX_CP_CLASS);
        
        HTable dataTable = new HTable(conf, dataTableName);
        System.out.println("run WWW in main(): ");
        HIndexConstantsAndUtils.updateIndexIndicator(conf, Bytes.toBytes(dataTableName), 1, true, cf, "country");
    }
}
