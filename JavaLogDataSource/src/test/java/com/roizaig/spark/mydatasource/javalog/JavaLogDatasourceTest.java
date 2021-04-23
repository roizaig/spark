package com.roizaig.spark.mydatasource.javalog;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by Roi Zaig on 2021-04-03.
 */
public class JavaLogDatasourceTest {

    private SparkSession sparkSession;

    @Before
    public void setUp() {
        sparkSession = SparkSession.builder()
                .appName("javalog_datasource_test")
                .master("local[*]")
                .getOrCreate();
    }

    @After
    public void tearDown() {
        sparkSession.close();
    }

    @Test
    public void javaLogDatasourceTest() {
        String filepath = "/home/roi.zaig/repos/roizaig/top/spark/JavaLogDataSource/src/main/resources/simplelog.log";
        String regexp = "(.+) \\[(.+)] (\\w+)\\s*\\[(.+):(\\d+)] - (.*)";
        Dataset<Row> df = sparkSession.read()
                .option(JavaLogUtils.JAVALOG_OPTION_FILENAME, filepath)
                .option(JavaLogUtils.JAVALOG_OPTION_REGEXP, regexp)
                .format("com.roizaig.spark.mydatasource.javalog")
                .load();

        df.show(false);

        assertEquals(6L, df.count());
        assertEquals(1L, df.select("Thread").distinct().count());
        assertEquals(3L, df.select("LogLevel").distinct().count());

        List<String> expectedLineNumbers = Arrays.asList(
                "349", "1215", "249", "107", "113", "57"
        );
        List<String> actualLineNumbers = df.select(df.col("LineNumber").cast(DataTypes.IntegerType))
                .collectAsList() // Java list
                .stream().map(row -> row.get(0).toString()) // extract the value from the Row object
                .collect(Collectors.toList());
        assertTrue(expectedLineNumbers.containsAll(actualLineNumbers));
        assertEquals(expectedLineNumbers.size(), actualLineNumbers.size());
    }

    @Test
    public void regexpTest() {
        String s = "2021-03-21 16:48:03.812 [        main] INFO  [alDataSwiftConnector:113] - === Done Processing swift2021.rje ===";
        String re = "(.+) \\[(.+)] (\\w+)\\s*\\[(.+):(\\d+)] - (.*)";

        Pattern p = Pattern.compile(re);
        Matcher m = p.matcher(s);
        System.out.println("Matches:    " + m.matches());
        System.out.println("Count  :    " + m.groupCount());
        for (int i = 1; i <= m.groupCount(); i++) {
            System.out.println("Group " + i + "  :    '" + m.group(i).trim() + "'");
        }

        assertEquals(6, m.groupCount());

    }

    @Test
    public void performanceTest() throws IOException {
        Path datafile = createTempDataFile();

        long start = System.currentTimeMillis();
        Dataset<String> df = sparkSession.read().textFile(datafile.toString());

        String re = "(.+) \\[(.+)] (\\w+)\\s*\\[(.+):(\\d+)] - (.*)";
        Dataset<Row> value = df.select(
                functions.regexp_extract(df.col("value"), re, 1).as("Date"),
                functions.regexp_extract(df.col("value"), re, 2).as("Thread"),
                functions.regexp_extract(df.col("value"), re, 3).as("LogLevel"),
                functions.regexp_extract(df.col("value"), re, 4).as("Class"),
                functions.regexp_extract(df.col("value"), re, 5).as("LineNumber"),
                functions.regexp_extract(df.col("value"), re, 6).as("Message")
        );
        value.show(false);
        System.out.println(value.count());
        long runTime1 = System.currentTimeMillis() - start;

        long start2 = System.currentTimeMillis();
        String regexp = "(.+) \\[(.+)] (\\w+)\\s*\\[(.+):(\\d+)] - (.*)";
        Dataset<Row> df2 = sparkSession.read()
                .option(JavaLogUtils.JAVALOG_OPTION_FILENAME, datafile.toString())
                .option(JavaLogUtils.JAVALOG_OPTION_REGEXP, regexp)
                .format("com.roizaig.spark.mydatasource.javalog")
                .load();
        df2.show(false);
        System.out.println(df2.count());
        long runTime2 = System.currentTimeMillis() - start2;

        System.out.println("Run time is: " + runTime1 + " ms");
        System.out.println("Run time is: " + runTime2 + " ms");

        Files.delete(datafile);
    }

    private Path createTempDataFile() throws IOException {
        Path datafile = Files.createTempFile(Paths.get("/tmp"), "simple_log", ".log");
        FileWriter fileWriter = new FileWriter(datafile.toString());
        PrintWriter printWriter = new PrintWriter(fileWriter);
        String[] dates = new String[]{
                "2021-03-21 16:48:03.811", "2021-03-21 16:48:03.823", "2021-03-21 16:48:03.824"
        };
        String[] logLevel = new String[]{
                "DEBUG", "ERROR", "INFO"
        };
        String[] className = new String[]{
                "entConnectionManager", "a.h.AmazonHttpClient", "dEncodingInputStream",
                "alDataConnector", "c.t.s.Application"
        };
        String[] lineNumber = new String[]{
                "349", "1215", "249", "107", "113", "57"
        };
        String[] message = new String[]{
                "Connection released: [id: 35][total kept alive: 0; route allocated: 0 of 500; total allocated: 0 of 500]",
                "Unable to execute HTTP request",
                "AwsChunkedEncodingInputStream reset (will reset the wrapped stream because it is mark-supported).",
                "Cannot upload the file to S3",
                "=== Done Processing file ===",
                "Started Application in 48.613 seconds (JVM running for 60.101)",
        };
        Random rand = new Random(System.currentTimeMillis());
        for (int i = 0; i < 10000000; i++) {
            printWriter.printf("%s [        main] %s [%s:%s] - %s\n",
                    dates[rand.nextInt(dates.length)],
                    logLevel[rand.nextInt(logLevel.length)],
                    className[rand.nextInt(className.length)],
                    lineNumber[rand.nextInt(lineNumber.length)],
                    message[rand.nextInt(message.length)]
            );
        }
        printWriter.flush();
        printWriter.close();
        return datafile;
    }


}