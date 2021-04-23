package com.roizaig.spark.mydatasource.javalog;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Created by Roi Zaig on 2021-03-17.
 */
public class JavaLogUtils {

    public static final String JAVALOG_OPTION_FILENAME = "filename";
    public static final String JAVALOG_OPTION_REGEXP = "regexp";

    private JavaLogUtils() {
    }

    public static StructType javaLogSchema() {
        return new StructType(
                new StructField[]{
                        new StructField("Date", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("Thread", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("LogLevel", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("Class", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("LineNumber", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("Message", DataTypes.StringType, true, Metadata.empty()),
                }
        );

    }


    static class MatcheException extends RuntimeException {

        private String text;
        private String pattern;
        private String filename;

        public MatcheException(String text, String pattern, String filename) {
            super(String.format("Error reading file %s. " +
                    "The pattern '%s' did not match the input: %s", filename, pattern, text));
            this.text = text;
            this.pattern = pattern;
            this.filename = filename;
        }

        public String getText() {
            return text;
        }

        public String getPattern() {
            return pattern;
        }

        public String getFilename() {
            return filename;
        }
    }

}
