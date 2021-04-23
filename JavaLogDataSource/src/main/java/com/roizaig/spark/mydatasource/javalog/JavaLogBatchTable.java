package com.roizaig.spark.mydatasource.javalog;

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Roi Zaig on 2021-03-17.
 */
public class JavaLogBatchTable implements SupportsRead {

    private static final Set<TableCapability> capabilities;

    static {
        capabilities = new HashSet<>(1);
        capabilities.add(TableCapability.BATCH_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new JavaLogScanBuilder(options);
    }

    @Override
    public String name() {
        return this.getClass().getName();
    }

    @Override
    public StructType schema() {
        return JavaLogUtils.javaLogSchema();
    }

    @Override
    public Set<TableCapability> capabilities() {
        return capabilities;
    }
}
