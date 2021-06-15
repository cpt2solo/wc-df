package ru.gb.batch.df;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class WordCountTest {

    private static SparkSession sqlc;

    @BeforeClass
    public static void beforeClass() {
        sqlc = SparkSession.builder()
                .appName("Word Count Test")
                .master("local[*]")
                .getOrCreate();
        Logger.getRootLogger().setLevel(Level.ERROR);
    }

    @AfterClass
    public static void afterClass() {
        sqlc.stop();
    }

    @Test
    public void countWords() {
        // init data
        List<Row> data = Arrays.asList(
                RowFactory.create("positive", "Good day"),
                RowFactory.create("neutral", "Rainy day")
        );
        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("class", DataTypes.StringType, false),
                DataTypes.createStructField("comment", DataTypes.StringType, false)
        ));
        Dataset<Row> df = sqlc.createDataFrame(data, schema);

        // transform data
        Dataset<Row> result = WordCount.countWords(df, " ");

        Map<String, Long> actual = result
                .collectAsList()
                .stream()
                .collect(Collectors.toMap(row -> row.getString(1), row -> row.getLong(0)));

        // validate
        Map<String, Long> expected = new HashMap<>();
        expected.put("positive", 1L);
        expected.put("neutral", 1L);
        expected.put("Good", 1L);
        expected.put("Rainy", 1L);
        expected.put("day", 2L);
        assertEquals(expected, actual);
    }
}
