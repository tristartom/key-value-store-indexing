package tthbase.client;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;

import java.util.*;

import tthbase.util.*;
import tthbase.client.*;

public class Demo {

    public static void initTables(Configuration conf, String testTableName, String columnFamily, String indexedColumnName) throws Exception{
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (!admin.isTableAvailable(testTableName)){
            HIndexConstantsAndUtils.createAndConfigBaseTable(conf, Bytes.toBytes(testTableName), Bytes.toBytes(columnFamily), new String[]{indexedColumnName});
        }

        byte[] indexTableName = HIndexConstantsAndUtils.generateIndexTableName(Bytes.toBytes(testTableName), Bytes.toBytes(columnFamily), Bytes.toBytes(indexedColumnName)/*TODO column family in index table*/);
        if (!admin.isTableAvailable(indexTableName)){
            HIndexConstantsAndUtils.createAndConfigIndexTable(conf, indexTableName, Bytes.toBytes(columnFamily));
        }
    }

    public static void initCoProcessors(Configuration conf, String coprocessorJarLoc, HTableGetByIndex htable) throws Exception {
       int coprocessorIndex = 1;
       HIndexConstantsAndUtils.updateCoprocessor(conf, htable.getTableName(), coprocessorIndex++, true, coprocessorJarLoc, "tthbase.coprocessor.IndexObserverwReadRepair");
       HIndexConstantsAndUtils.updateCoprocessor(conf, htable.getTableName(), coprocessorIndex++, true, coprocessorJarLoc, "tthbase.coprocessor.PhysicalDeletionInCompaction");
       htable.configPolicy(HTableGetByIndex.PLY_READCHECK);
    }

    public static void main(String[] args) throws Exception{
       Configuration conf = HBaseConfiguration.create();
       String testTableName = "testtable";
       String columnFamily = "cf";
       String indexedColumnName = "country";

       if(args.length <= 0){
           System.err.println("format: java -cp <classpath> tthbase.client.Demo <where coproc.jar is>");
           System.err.println("example: java -cp build/jar/libDeli-client.jar:conf:lib/hbase-binding-0.1.4.jar tthbase.client.Demo  /root/app/deli/build/jar/libDeli-coproc.jar ");
           return;
       }
       String locCoproc = args[0];
       String coprocessorJarLoc = "file:" + locCoproc;

       initTables(conf, testTableName, columnFamily, indexedColumnName);
       HTableGetByIndex htable = new HTableGetByIndex(conf, Bytes.toBytes(testTableName));
       initCoProcessors(conf, coprocessorJarLoc, htable);

       //put value1
       Put p = new Put(Bytes.toBytes("key1"));
       p.add(Bytes.toBytes(columnFamily), Bytes.toBytes(indexedColumnName), 101, Bytes.toBytes("v1"));
       htable.put(p);

       //getByIndex
       htable.configPolicy(HTableGetByIndex.PLY_FASTREAD);
       List<byte[]> res = htable.getByIndex(Bytes.toBytes(columnFamily), Bytes.toBytes(indexedColumnName), Bytes.toBytes("v1"));
       assert(res != null && res.size() != 0);
       System.out.println("Result is " + Bytes.toString(res.get(0)));
    }
}
