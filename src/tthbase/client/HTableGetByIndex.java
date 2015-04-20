package tthbase.client;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import java.io.IOException;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

import tthbase.commons.HTableWithIndexesDriver;

public class HTableGetByIndex extends HTableWithIndexesDriver {

    int policyReadIndex;
    public static final int PLY_FASTREAD = 0;
    public static final int PLY_READCHECK = 1;

    public HTableGetByIndex (Configuration conf, byte[] tableName) throws IOException {
        super(conf, tableName);
        //default is baseline
        configPolicy(PLY_FASTREAD);
    }

    public void configPolicy(int p){
        policyReadIndex = p;
    }

    public int getIndexingPolicy(){
        return policyReadIndex;
    }

    public List<byte[]> getByIndex(byte[] columnFamily, byte[] columnName, byte[] value) throws IOException {
        List<byte[]> rawResults = readIndexOnly(columnFamily, columnName, value);
        List<byte[]> datakeyToDels = new ArrayList<byte[]>();
        if(policyReadIndex == PLY_READCHECK) {
            //perform read repair
            if (rawResults != null) {
                for(byte[] dataRowkey : rawResults) {
                    //read in base table to verify
                    byte[] valueFromBase = readBase(dataRowkey, columnFamily, columnName);
                    if(!Bytes.equals(valueFromBase, value)) {
                        datakeyToDels.add(dataRowkey);
                    }
                }
                rawResults.removeAll(datakeyToDels);
                for(byte[] datakeyToDel : datakeyToDels){
                    deleteFromIndex(columnFamily, columnName, value, datakeyToDel);
                }
            }
        }
        return rawResults;
    }

    private byte[] readBase(byte[] dataRowkey, byte[] columnFamily, byte[] columnName) throws IOException {
        Get g = new Get(dataRowkey);
        g.addColumn(columnFamily, columnName);
        Result r = this.get(g);
        assert r.raw().length == 1;
        KeyValue keyValue = r.raw()[0];
        return keyValue.getValue();
    }

    private List<byte[]> readIndexOnly(byte[] columnFamily, byte[] columnName, byte[] value) throws IOException {
        assert value != null;
        Map<byte[], List<byte[]> > res = internalGetByIndexByRange(columnFamily, columnName, value, null);
        if(res == null || res.size() ==0) {
            return null;
        } else {
//System.out.print("index read res-" + res.size() + ": ");
List<byte[]> toRet = new ArrayList<byte[]>();
for(Map.Entry<byte[], List<byte[]> > e : res.entrySet())
{
//  System.out.print(Bytes.toString(e.getKey()));
  List<byte[]> keys = e.getValue();
//  System.out.print("=>{");
  for(byte[] key : keys){
    toRet.add(key);
//    System.out.print(Bytes.toString(key) + ",");
  }
//  System.out.print("}");
}
//System.out.println();
            return toRet;
        }
    }

//note valueEnd is inclusive.
//TODO
    public Map<byte[], List<byte[]> > getByIndexByRange(byte[] columnFamily, byte[] columnName, byte[] valueStart, byte[] valueEnd) throws IOException {
        assert valueStart != null;
        assert valueEnd != null;
        assert Bytes.toString(valueStart).compareTo(Bytes.toString(valueEnd)) < 0; //assert valueStart < valueEnd, lexicographically;
        return internalGetByIndexByRange(columnFamily, columnName, valueStart, Bytes.toBytes(Bytes.toString(valueEnd) + "0"));
    }
}
