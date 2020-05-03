package com.learning

import scala.annotation.tailrec

object Exercises extends App {
  def exercise1(): Unit = {
    val doublePi = Math.PI * 2

    println(f"Pi doubled value is $doublePi%.3f")
  }

  def exercise2(fibNo: Int): Unit = {
    def Fib(num: Int): Int = {
      @tailrec def auxFib(i: Int, accum:Int, accum2:Int): Int = {
        if(i >= num) accum
        else auxFib(i+1, accum + accum2, accum)
      }
      if(num <= 2) 1
      else auxFib(2, 1, 1)
    }

    for(x <- 1 to 9) {
      println(Fib(x))
    }
  }

  def exercise3(): Unit = {
    def upperString(str: String): String = str.toUpperCase
    val literal : String => String = x => x.toUpperCase

    println(upperString("test"))
    println(literal("test2"))
  }

  def exercise4(): Unit = {
    val aList = List.range(1,21)

    println("Iterative Way")
    for(itm <- aList){
      if(itm % 3 == 0){
        println(itm)
      }
    }

    println("Filtering Way")
    val filteredList = aList.filter(_ % 3 == 0)
    filteredList.foreach(println)
  }

  exercise1
  exercise2(10)
  exercise3
  exercise4
}
