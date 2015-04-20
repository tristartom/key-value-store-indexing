package tthbase.commons;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

// specific to index materialization by composite index rowkey (i.e., indexRowKey=value/key)
public class MaterializeIndexByCompositeRowkey implements MaterializeIndex {
//using scanner
    public Map<byte[], List<byte[]> > getByIndexByRange(HTable indexTable, byte[] valueStart, byte[] valueStop) throws IOException {
        //read against index table
        Scan scan = new Scan();
        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL); //return rows that meet all filter conditions. (AND)
        fl.addFilter(new FirstKeyOnlyFilter());//return first instance of a row, then skip to next row. (avoiding rows of the same keys).
        fl.addFilter(new KeyOnlyFilter());// return only the key, not the value.
        assert valueStart != null;
        scan.setStartRow(valueStart);
        if (valueStop == null){ //point query
            Filter prefixFilter = new PrefixFilter(valueStart);
            fl.addFilter(prefixFilter);
        } else {
            scan.setStopRow(valueStop);
        }
        scan.setFilter(fl);

        ResultScanner scanner = indexTable.getScanner(scan);
        //ResultScanner is for client-side scanning.
        Map<byte[], List<byte[]> > toRet = new HashMap<byte[], List<byte[]>>();
        for (Result r : scanner) {
            if(r.raw().length == 0) continue;
            for (KeyValue kv : r.raw()) {
                byte[] indexRowkey = kv.getRow();
                String [] parsedIndexRowkey = IndexStorageFormat.parseIndexRowkey(indexRowkey);
                byte[] dataValue = Bytes.toBytes(parsedIndexRowkey[0]);
                byte[] dataKey = Bytes.toBytes(parsedIndexRowkey[1]);

                if(toRet.get(dataValue) == null){
                    List<byte[]> results = new ArrayList<byte[]>();
                    results.add(dataKey);
                    toRet.put(dataValue, results);
                } else {
                    toRet.get(dataValue).add(dataKey);
                }
            }
        }
        scanner.close();
        return toRet;
    }

    public void putToIndex(HTable indexTable, byte[] dataValue, byte[] dataKey) throws IOException {
        byte[] indexRowkey = IndexStorageFormat.generateIndexRowkey(dataKey, dataValue);
        Put put2Index = new Put(indexRowkey);
        put2Index.add(IndexStorageFormat.INDEXTABLE_COLUMNFAMILY, IndexStorageFormat.INDEXTABLE_SPACEHOLDER, IndexStorageFormat.INDEXTABLE_SPACEHOLDER);
        indexTable.put(put2Index);
    }

    public void deleteFromIndex(HTable indexTable, byte[] dataValue, byte[] dataKey) throws IOException {
        byte[] indexRowkey = IndexStorageFormat.generateIndexRowkey(dataKey, dataValue);
        Delete del = new Delete(indexRowkey);
        //del.setTimestamp(timestamp);
        indexTable.delete(del);
    }
}

class IndexStorageFormat {
//TOREMOVE below is specific to implemention by composedIndexRowkey (rowkey=value/key)
    static final String INDEX_ROWKEY_DELIMITER = "/";

    static final public byte[] INDEXTABLE_COLUMNFAMILY = Bytes.toBytes("cf"); //be consistent with column_family_name in weblog_cf_country (in current preloaded dataset)
    static final public byte[] INDEXTABLE_SPACEHOLDER = Bytes.toBytes("EMPTY");

    static String[] parseIndexRowkey(byte[] indexRowkey){
        return Bytes.toString(indexRowkey).split(INDEX_ROWKEY_DELIMITER);
    }

    static byte[] generateIndexRowkey(byte[] dataKey, byte[] dataValue){
        return Bytes.toBytes(Bytes.toString(dataValue) + INDEX_ROWKEY_DELIMITER + Bytes.toString(dataKey));
    }
}
