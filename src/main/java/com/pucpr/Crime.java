package com.pucpr;

import scala.Tuple2;

import java.io.Serializable;

public class Crime implements Serializable {

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

    static void showAnswer(String a, Object b){
        System.out.println(a + " " + b);
    }

}
