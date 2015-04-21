package tthbase.coprocessor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HTableDescriptor;

import java.io.IOException;
import java.util.List;

import tthbase.util.HIndexConstantsAndUtils;
import tthbase.commons.HTableWithIndexesDriver;

public class HTableUpdateIndexByPut extends HTableWithIndexesDriver {
    public HTableUpdateIndexByPut(Configuration conf, byte[] tableName) throws IOException {
        super(conf, tableName);
    }

    final static private int INSERT_INDEX = 0;
    final static private int READ_BASE = 1;
    final static private int DELETE_INDEX = 2;

    /**
        @parameter, if DELETE_INDEX, readResult4Delete is not null, otherwise, null.
        @return, if READ_BASE, return read result; otherwise, null.
    */
    private Result internalPrimitivePerPut(Put put, int mode, Result readResult4Delete) throws IOException {
        HTableDescriptor dataTableDesc = null;
        try {
            dataTableDesc = getTableDescriptor();
        } catch (IOException e1) {
            throw new RuntimeException("TTERROR" + (errorIndex++) + "_DETAIL: " + e1.getMessage());
        }

        byte[] dataKey = put.getRow();
        Get get = null;
        if (mode == READ_BASE) {
             get = new Get(dataKey);
        }

        for (int index = 1; ; index++) {
            String fullpathOfIndexedcolumnInDatatable = dataTableDesc.getValue(HIndexConstantsAndUtils.INDEX_INDICATOR + index);
            if(fullpathOfIndexedcolumnInDatatable == null){
                //no (further) index column, stop at current index
                break;
            } else {
                String[] datatableColumnPath = fullpathOfIndexedcolumnInDatatable.split("\\|");
                byte[] indexedColumnFamily = Bytes.toBytes(datatableColumnPath[0]);
                byte[] indexedColumnName = Bytes.toBytes(datatableColumnPath[1]); 
                byte[] dataValuePerColumn = getColumnValue(put, indexedColumnFamily, indexedColumnName);
                if(dataValuePerColumn != null){
                    if(mode == INSERT_INDEX){
                        //put new to index
                        putToIndex(indexedColumnFamily, indexedColumnName, dataValuePerColumn, dataKey);
                    } else if (mode == READ_BASE) {
                        //read base 
                        //TOREMOVE need specify timestamp to guarantee get old values.
                        get.addColumn(indexedColumnFamily, indexedColumnName);
                    } else { // DELETE_INDEX
                        //delete old from index
                        Result readResultOld = readResult4Delete;
                        byte[] oldDataValuePerColumn = readResultOld.getValue(indexedColumnFamily, indexedColumnName);
                        deleteFromIndex(indexedColumnFamily, indexedColumnName, oldDataValuePerColumn, dataKey);
                    }
                } else {
                    //the indexed column (family) is not associated with the put, to continue.
                    continue;
                }
            }
        }
        if (mode == READ_BASE) {
             Result readResultOld = this.get(get);
             return readResultOld;
        } else {
             return null;
        }
    }

    public void insertNewToIndexes(Put put) throws IOException {
        internalPrimitivePerPut(put, INSERT_INDEX, null);
    }

    public void readBaseAndDeleteOld(Put put) throws IOException {
        Result readBaseResult = internalPrimitivePerPut(put, READ_BASE, null);
        internalPrimitivePerPut(put, DELETE_INDEX, readBaseResult);
    }

//TOREMOVE does it belong to HTableWithIndexesDriver?
//it gets one and only one version.
    protected byte[] getColumnValue(final Put put, byte[] columnFamily, byte[] columnName){
        if(!put.has(columnFamily, columnName)){
            return null;
        }

        List<Cell> values = put.get(columnFamily, columnName);
        if (values == null || values.isEmpty()) {
            throw new RuntimeException("TTERROR_" + (errorIndex++) + ": " + "empty value lists while put.has() returns true!");
        }

        //should be one element in values, since column qualifier is an exact name, matching one column; also one version of value is expected.
        if (values.size() != 1) {
            throw new RuntimeException("TTERROR_" + (errorIndex++) + ": " + "multiple versions of values or multiple columns by qualier in put()!");
        }

//TOREMOVE to get timestamp, refer to old project code.
        Cell cur = values.get(0);
        byte[] value = cur.getValue();
        return value;
    }
}
