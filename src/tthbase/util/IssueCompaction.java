package tthbase.util;
import tthbase.client.HTableGetByIndex;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import tthbase.util.HIndexConstantsAndUtils;
import java.util.List;
import java.io.IOException;

/*
disable 'testtable2'
drop 'testtable2'
create 'testtable2', {NAME=>'cf', KEEP_DELETED_CELLS=>true}
disable 'testtable2_cf_country'
drop 'testtable2_cf_country'
create 'testtable2_cf_country', "cf"

disable 'testtable2'
alter 'testtable2', METHOD => 'table_att', 'coprocessor' => 'hdfs://node1:8020/hbase_cp/libHbaseCoprocessor.jar|tthbase.coprocessor.PhysicalDeletionInCompaction|1001|arg1=1,arg2=2'
alter 'testtable2', METHOD => 'table_att', 'secondaryIndex$1' => 'cf|country'
enable 'testtable2'

put 'testtable2', "key1", 'cf:country', "v1", 101
flush 'testtable2' #hbase compaction ignores all data from memstore.
put 'testtable2', "key1", 'cf:country', "v2", 102
flush 'testtable2' #hbase compaction ignores all data from memstore.
major_compact 'testtable2'
get 'testtable2', 'key1', {COLUMN => 'cf:country', VERSIONS => 4} #1 version
describe 'testtable2'
*/

public class IssueCompaction {
    static byte[] columnFamily = Bytes.toBytes("cf");
    static String indexedColumnName = "country";
    static byte[] indexTableName = null;
    static String coprocessorJarLoc = "hdfs://node1:8020/hbase_cp/libHbaseCoprocessor.jar";

    static HTableGetByIndex htable = null;
    static Configuration conf = null;
    static byte[] rowKey = Bytes.toBytes("key1");

    static void fakedCreateTable(byte[] dataTableName) throws IOException{
        //flush and compact
        //setup
        HIndexConstantsAndUtils.createAndConfigBaseTable(conf, dataTableName, columnFamily, new String[]{indexedColumnName});
        //create index table
        HIndexConstantsAndUtils.createAndConfigIndexTable(conf, indexTableName, columnFamily);
        htable = new HTableGetByIndex(conf, dataTableName);
    }

    static void fakedLoadData(byte[] dataTableName) throws IOException{
        //load data
        //put value1
        Put p = new Put(rowKey);
        p.add(columnFamily, Bytes.toBytes(indexedColumnName), 101, Bytes.toBytes("v1"));
        htable.put(p);

        //put value2
        p = new Put(rowKey);
        p.add(columnFamily, Bytes.toBytes(indexedColumnName), 102, Bytes.toBytes("v2"));
        htable.put(p);
    }

    public static void issueMajorCompactionAsynchronously(byte[] dataTableName){
        try{
            HBaseAdmin admin = new HBaseAdmin(conf);
            //admin.flush(dataTableName);
            admin.majorCompact(dataTableName);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static void fakedTest(byte[] dataTableName){
        try{
            //get th'm all
            Get g = null;
            Result res = null;
            g = new Get(rowKey);
            g.addColumn(columnFamily, Bytes.toBytes(indexedColumnName));
            g.setMaxVersions();
            res = htable.get(g);
            List<KeyValue> rl = res.getColumn(columnFamily, Bytes.toBytes(indexedColumnName));
            System.out.println("TTDEBUG: " + rl.size());
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static void fakedTeardown(byte[] dataTableName){
        try{
            //teardown
            if(htable != null){
                htable.close();
            }
            HIndexConstantsAndUtils.deleteTable(conf, indexTableName);
            HIndexConstantsAndUtils.deleteTable(conf, dataTableName);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if(args == null || args.length != 2){
            System.out.println("format: 0.tablename 1.[yes|no]if_load_coprocessor_physical_deletion");
            System.out.println("example: java tthbase.util.IssueMajorCompactionAsynchronously testtable yes");
            return;
        }
        byte[] dataTableName = Bytes.toBytes(args[0]);
        boolean loadCoprocessor = "yes".equals(args[1]);
//        System.out.println("TTDEBUG tablename=" + Bytes.toString(dataTableName) + ", if2loadcoprocessor=" + loadCoprocessor);
        indexTableName = HIndexConstantsAndUtils.generateIndexTableName(dataTableName, columnFamily, Bytes.toBytes(indexedColumnName)/*TODO column family in index table*/);
        conf = HBaseConfiguration.create();

        try{
            if (loadCoprocessor){
                HIndexConstantsAndUtils.updateCoprocessor(conf, dataTableName, 1, true, coprocessorJarLoc, "tthbase.coprocessor.PhysicalDeletionInCompaction");
            } else {
                HIndexConstantsAndUtils.updateCoprocessor(conf, dataTableName, 1, false, null, null);
            }
            issueMajorCompactionAsynchronously(dataTableName);
       } catch(Exception e){
            e.printStackTrace();
       }
    }
}
