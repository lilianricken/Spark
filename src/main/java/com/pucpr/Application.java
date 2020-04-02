package com.pucpr;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

public class Application {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("pratica");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        ctx.setLogLevel("ERROR");
        JavaRDD<String> file = ctx.textFile("src/main/resources/ocorrencias_criminais.csv");

        JavaRDD<Crime> dataSet = file.map(s -> {
            String[] campos = s.split(";");
            int dia = Integer.parseInt(campos[0]);
            int mes = Integer.parseInt(campos[1]);
            int ano = Integer.parseInt(campos[2]);
            String tipo = campos[4];
            return new Crime(dia, mes, ano, tipo);
        }).cache();
        long count = dataSet.count();

        //1. Quantidade de crimes por ano
        JavaRDD<Integer> porAno = dataSet.map(crime -> crime.ano);
        showAnswer("Quantidade de crimes por ano: ", porAno.countByValue());

        //2. Quantidade de crimes por ano que sejam do tipo NARCOTICS
        JavaRDD<Crime> narcotics = dataSet.filter(crime -> crime.tipo.equalsIgnoreCase("NARCOTICS"));
        showAnswer("Crimes do tipo NARCOTICS por ano: ", narcotics.map(crime -> crime.ano).countByValue());

        //3. Quantidade de crimes por ano, que sejam do tipo NARCOTICS, e tenham ocorrido em dias pares
        showAnswer("Crimes do tipo NARCOTICS em dias pares: ", narcotics
                .filter(crime -> (crime.dia % 2) == 0)
                .map(crime -> crime.ano)
                .countByValue());

        //4. Mês com maior ocorrência de crimes
        JavaPairRDD<Integer, Float> mes = dataSet
                .mapToPair(crime -> new Tuple2<>(crime.mes, 1F))
                .reduceByKey(Float::sum);
        showAnswer("Mês com maior ocorrência de crimes: ",
                mes.reduce(Application::max));

        //5. Mês com a maior média de ocorrência de crimes;
        showAnswer("Mês com a maior média de ocorrencias: ",
                mes.map(a -> new Tuple2<>(a._1, (a._2 / count) * 100))
                        .reduce(Application::max));

        //6. Mês por ano com a maior ocorrência de crimes;
        List<Tuple2<String, Integer>> mesAno = dataSet.map(crime ->
                crime.ano + "/" + crime.mes)
                .mapToPair(s -> new Tuple2<>(s.split(";")[0], 1))
                .reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap)
                .take(5)
                ;
        showAnswer("Crimes por mês de cada ano", mesAno);
        /*Professor: a partir deste ponto não consegui retirar da lista o mês com o maior caso de
        ocorrencias de cada ano. Acredito que isso possa ser feito com flatmap, mas como mandei em
        mensagem e pelo forum e não obtive resposta estou entregando até onde consegui fazer
         */

        //7. Mês com a maior ocorrência de crimes do tipo “DECEPTIVE PRACTICE”
        JavaRDD<Crime> decPractice = dataSet
                .filter(crime -> crime.tipo
                        .equalsIgnoreCase("DECEPTIVE PRACTICE"));
        showAnswer("Crimes do tipo DECEPTIVE PRACTICE por ano: ",
                decPractice.map(crime -> crime.mes).countByValue());

        //8. Dia do ano com a maior ocorrência de crimes;
        JavaRDD<String> diaMes = dataSet.map(crime ->
                crime.dia + "/" + crime.mes);
        JavaPairRDD<String, Integer> data = diaMes
                .mapToPair(s -> new Tuple2<>(s.split(";")[0], 1))
                .reduceByKey(Integer::sum)
                .sortByKey(false);
        showAnswer("Dia do ano com a maior ocorrência de crimes: ", data.first());

        ctx.stop();
    }


    public static <T> Tuple2<T, Float> max(Tuple2<T, Float> x, Tuple2<T, Float> y) {
        if (x._2 > y._2) return x;
        return y;
    }

    public static void showAnswer(String a, Object b) {
        System.out.println(a + " " + b);
    }

    public static class Crime implements Serializable {

        int dia;
        int mes;
        int ano;
        String tipo;

        public Crime(int dia, int mes, int ano, String tipo) {
            this.dia = dia;
            this.mes = mes;
            this.ano = ano;
            this.tipo = tipo;
        }

        public int getDia() {
            return dia;
        }

        public int getMes() {
            return mes;
        }

        public int getAno() {
            return ano;
        }

        public String getTipo() {
            return tipo;
        }
    }
}
