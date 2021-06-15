package ru.gb.batch.df;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import static org.apache.spark.sql.functions.*;
import  org.apache.spark.api.java.function.FilterFunction;

import java.util.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Класс запускает Spark RDD задачу, которая:
 * 1. читает каждый файл из директории в args[0]
 * 2. разбивает каждую строку на слова разделителем args[3]
 * 3. из всех слов в файле составляет пару вида "слово-1"
 * 4. суммирует все числа у одинаковых слов и записывает результат в файл args[1]
 */
public class WordCount {

    /**
     * Входная точка приложения. Считает количество слов во входном файле и пишет результат в выходной.
     */
    public static void main(String[] args) throws IOException {
        // проверка аргументов
        if (args.length < 3) {
            throw new IllegalArgumentException("Expected arguments: input_dir output_dir delimiter [stopwords]");
        }
        final String input = args[0];
        final String output = args[1];
        final String delimiter = args.length > 2 ? args[2] : " ";
        final Set<String> stopWords = args.length > 3 ? new HashSet<>(Files.readAllLines(Paths.get(args[3]))) : Collections.emptySet();

        // инициализация Spark
        SparkSession sqlc = SparkSession.builder().getOrCreate();

        // выполняем broadcast и открываем файл на чтение
        Dataset<Row> df = sqlc.read().option("header", "true").csv(input);

        // вызываем функцию, которая преобразует данные
        Dataset<Row> wc = countWords(df, delimiter, stopWords);

        // сохраняем на диск
        wc.write().mode(SaveMode.Overwrite).csv(output);

        // завершаем работу
        sqlc.stop();
    }

    /**
     * Функция получает на вход {@code rdd} со документами, которые разбивает на термы через {@code delimiter},
     * удаляет слова из стоп-списка слов {@code stopWords},
     * после чего считает количество повторений каждого терма.
     */
    static Dataset<Row> countWords(Dataset<Row> df, String delimiter, Set<String> stopWords) {
        JavaSparkContext sc = new JavaSparkContext(df.sqlContext().sparkContext());
        Broadcast<Set<String>> broadcast = sc.broadcast(stopWords);
        return df.select(concat_ws(" ", col("class"), col("comment")).as("docs"))
                .select(split(col("docs"), delimiter).as("words"))
                .select(explode(col("words")).as("word"))
                .filter((FilterFunction<Row>) row -> !broadcast.getValue().contains(row.getString(0)))
                .groupBy(col("word")).count();
    }

}
