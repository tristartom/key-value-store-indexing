package tthbase.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.*;
/**
to view the printout, see it in logs/*.log file
*/

public class LoggedObserver extends BaseRegionObserver {
    public static final Log LOG = LogFactory.getLog(HRegion.class);

    private boolean functionLevelLoggingEnabled = true;

    public void setFunctionLevelLogging(boolean b) {
        functionLevelLoggingEnabled = b;
    }

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        if(functionLevelLoggingEnabled){
            LOG.debug("TTDEBUG_FUNC: start()");
        }
    }
 
    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
        if(functionLevelLoggingEnabled){
            LOG.debug("TTDEBUG_FUNC: postOpen()");
        }
    }

    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e, final Put put, final WALEdit edit, final Durability durability) throws IOException {
        if(functionLevelLoggingEnabled){
            LOG.debug("TTDEBUG_FUNC: prePut()");
        }
    }
  
    @Override
    public void postPut(final ObserverContext<RegionCoprocessorEnvironment> e, final Put put, final WALEdit edit, final Durability durability) throws IOException {
        if(functionLevelLoggingEnabled){
            LOG.debug("TTDEBUG_FUNC: postPut()");
        }
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, final Durability durability) throws IOException {
        if(functionLevelLoggingEnabled){
            LOG.debug("TTDEBUG_FUNC: preDelete()");
        }
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> result) throws IOException {
        if(functionLevelLoggingEnabled){
            LOG.debug("TTDEBUG_FUNC: preGet()");
        }
    }

    @Override
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> result) throws IOException {
        if(functionLevelLoggingEnabled){
            LOG.debug("TTDEBUG_FUNC: postGet()");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException { 
        if(functionLevelLoggingEnabled){
            LOG.debug("TTDEBUG_FUNC: stop()");
        }
    }

    //new features in hbase-0.94.2

    @Override
    public InternalScanner preCompactScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, Store store, List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs, InternalScanner s) throws IOException {
        if(functionLevelLoggingEnabled){
            LOG.debug("TTDEBUG_FUNC: preCompactScannerOpen()");
        }
        return null; //return null to perform default processing.
    }

    @Override
    public KeyValueScanner preStoreScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, final Store store, final Scan scan, final NavigableSet<byte[]> targetCols, final KeyValueScanner s) throws IOException {
        if(functionLevelLoggingEnabled){
            LOG.debug("TTDEBUG_FUNC: preStoreScannerOpen()");
        }
        return null;
    }

    @Override
    public InternalScanner preFlushScannerOpen(final ObserverContext<RegionCoprocessorEnvironment> c, Store store, KeyValueScanner memstoreScanner, InternalScanner s) throws IOException {
        if(functionLevelLoggingEnabled){
            LOG.debug("TTDEBUG_FUNC: preFlushScannerOpen()");
        }
        return null;
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> c, Store store, StoreFile resultFile) {
        if(functionLevelLoggingEnabled){
            LOG.debug("TTDEBUG_FUNC: postCompact()");
        }
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,
      final Store store, final InternalScanner scanner, final ScanType scanType) throws IOException{
        if(functionLevelLoggingEnabled){
            LOG.debug("TTDEBUG_FUNC: preCompact()");
        }
        return scanner; 
        //Returns: the scanner to use during compaction. Should not be null unless the implementation is writing new store files on its own. 
    }

    @Override
    public void preClose(ObserverContext<RegionCoprocessorEnvironment> e, boolean abortRequested) { 
        if(functionLevelLoggingEnabled){
            LOG.debug("TTDEBUG_FUNC: preClose()");
        }
    }

}
