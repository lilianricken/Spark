package com.pucpr;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.List;

import static com.pucpr.Crime.showAnswer;

public class Application {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("pratica");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        ctx.setLogLevel("ERROR");
        JavaRDD<String> file = ctx.textFile("src/main/resources/ocorrencias_criminais_sample.csv");

        JavaRDD<Crime> dataSet = file.map(s -> {
            String[] campos = s.split(";");
            int dia = Integer.parseInt(campos[0]);
            int mes = Integer.parseInt(campos[1]);
            int ano = Integer.parseInt(campos[2]);
            String tipo = campos[4];
            return new Crime(dia, mes, ano, tipo);
        });
        dataSet.cache();
        long count = dataSet.count();

        //qtdade crimes por ano
        JavaRDD<Integer> byAno = dataSet.map(crime -> crime.ano);
        showAnswer("Quantidade de crimes por ano: ", byAno.countByValue());

        //Quantidade de crimes por ano que sejam do tipo NARCOTICS
        JavaRDD<Crime> narcotics = dataSet.filter(crime -> crime.tipo.equalsIgnoreCase("NARCOTICS"));
        showAnswer("Crimes do tipo NARCOTICS por ano", narcotics.map(crime -> crime.ano).countByValue());

        //Quantidade de crimes por ano, que sejam do tipo NARCOTICS, e tenham ocorrido em dias pares;
        showAnswer("Por narcoticos dias pares: ", narcotics.filter(crime -> {
            int dia = crime.dia;
            return (dia % 2) == 0;
        })
                .map(crime -> crime.ano)
                .countByValue());

        //Mês com maior ocorrência de crimes;
        JavaPairRDD<Integer, Float> pairs = dataSet.mapToPair(crime -> new Tuple2<>(crime.mes, 1F));
        JavaPairRDD<Integer, Float> months = pairs.reduceByKey(Float::sum);
        showAnswer("Mês com a maior ocorrencia de crimes",
                months.reduce(Application::max));

        //Mês com a maior média de ocorrência de crimes;
        showAnswer("Mês com a maior média de ocorrencias",
                months.map(a -> new Tuple2<>(a._1, (a._2 / count) * 100))
                        .reduce(Application::max));

        //Mês por ano com a maior ocorrência de crimes;
        JavaRDD<Tuple3<Integer, Integer, Integer>> mes
        //Mês com a maior ocorrência de crimes do tipo “DECEPTIVE PRACTICE”
        //Dia do ano com a maior ocorrência de crimes;
        ctx.stop();
    }

    private static Tuple2<Integer, Float> max(Tuple2<Integer, Float> x, Tuple2<Integer, Float> y) {
        if (x._2 > y._2) return x;
        return y;
    }

}
