package tthbase.util;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HConstants;

import java.io.IOException;

public class HIndexConstantsAndUtils {
    //TODO refactor: to match with HTableWithIndexesDriver.java
    //TODO refactor: to match with tthbase.util.UpdateCoprocessor.java
    public static final String INDEX_INDICATOR = "secondaryIndex$";
    public static final String INDEX_DELIMITOR = "|";
    //TODO make it a static function in commons.jar.
    public static byte[] generateIndexTableName(byte[] dataTableName, byte[] columnFamily, byte[] columnName){
        return Bytes.toBytes(Bytes.toString(dataTableName) + '_' + Bytes.toString(columnFamily) + '_' + Bytes.toString(columnName));
    }

    //TODO copied from and refined based on UpdateCoprocessor.java
    public static void updateCoprocessor(Configuration conf, byte[] tableName, int indexOfCoprocessor, boolean ifUpdateorRemove, String coprocessorLoc, String coprocessorClassname) throws IOException{
        String rawAttributeName = "coprocessor$";
        String value = coprocessorLoc + "|" /*Not index delimitor*/+ coprocessorClassname + "|1001|arg1=1,arg2=2";
        updateTableAttribute(conf, tableName, rawAttributeName, indexOfCoprocessor, ifUpdateorRemove, value);
    }

    public static void updateIndexIndicator(Configuration conf, byte[] tableName, int indexOfIndexIndicator, boolean ifUpdateorRemove, String indexedCF, String indexedColumn) throws IOException{
        String rawAttributeName = INDEX_INDICATOR;
        String value = indexedCF + INDEX_DELIMITOR + indexedColumn;
        updateTableAttribute(conf, tableName, rawAttributeName, indexOfIndexIndicator, ifUpdateorRemove, value);
    }

    /**
    @param rawAttributeName is the attribute name viewed by applications, it allows multiple values. For example, secondaryIndex in secondaryIndex$1 and coprocessor in corpcessor$2
    @param indexOfAttribute is of the same raw attribute name, for example 2 in secondary$2
    */
    static void updateTableAttribute(Configuration conf, byte[] tableName, String rawAttributeName, int indexOfAttribute, boolean ifUpdateorRemove, String value) throws IOException{
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor desc = admin.getTableDescriptor(tableName);
        admin.disableTable(tableName);
//        System.out.println("TTDEBUG: disable table " + Bytes.toString(tableName));
        String coprocessorKey = rawAttributeName + indexOfAttribute;
        if(!ifUpdateorRemove) {
            desc.remove(Bytes.toBytes(coprocessorKey));
        } else {
            desc.setValue(coprocessorKey, value);
        }
        admin.modifyTable(tableName, desc);
//        System.out.println("TTDEBUG: modify table " + Bytes.toString(tableName));
        admin.enableTable(tableName);
//        System.out.println("TTDEBUG: enable table " + Bytes.toString(tableName));
        HTableDescriptor descNew = admin.getTableDescriptor(tableName);
        //modify table is asynchronous, has to loop over to check
        while (!desc.equals(descNew)){
            System.err.println("TTDEBUG: waiting for descriptor to change: from " + descNew + " to " + desc);
            try {Thread.sleep(500);} catch(InterruptedException ex) {}
            descNew = admin.getTableDescriptor(tableName);
        }
    }

    public static void createAndConfigBaseTable(Configuration conf, byte[] tableName, byte[] columnFamily, String[] indexedColumnNames) throws IOException{
        //create a table with column familiy columnFamily
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor desc = new HTableDescriptor(tableName);
        //specify indexable columns.
        for (int i = 0; i < indexedColumnNames.length; i ++){
            desc.setValue(INDEX_INDICATOR + (i + 1), Bytes.toString(columnFamily) + INDEX_DELIMITOR + indexedColumnNames[i]);
        }
        HColumnDescriptor descColFamily = new HColumnDescriptor(columnFamily);
        //configure to set KEEP_DELETED_CELLS => 'true'
        descColFamily.setKeepDeletedCells(true);
        descColFamily.setTimeToLive(HConstants.FOREVER);
        descColFamily.setMaxVersions(Integer.MAX_VALUE);

        desc.addFamily(descColFamily);
        admin.createTable(desc);
    }

    public static void createAndConfigIndexTable(Configuration conf, byte[] tableName, byte[] columnFamily) throws IOException{
        //create a table with column familiy columnFamily
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor desc = new HTableDescriptor(tableName);
        //configure to set KEEP_DELETED_CELLS => 'true'
        HColumnDescriptor descColFamily = new HColumnDescriptor(columnFamily);
        desc.addFamily(descColFamily);
        admin.createTable(desc);
    }


    public static void deleteTable(Configuration conf, byte[] tableName) throws IOException{
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }
}
