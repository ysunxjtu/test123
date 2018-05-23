package com.sunyao.test.elasticsearch;

import java.util.Arrays;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        //System.out.println( "Hello World!" );

        //1.使用lambda表达式对列表进行迭代
       /* List features = Arrays.asList("Lambdas", "Default Method", "Stream API", "Date and Time API");
        features.forEach((n) -> System.out.println(n));*/

        //2.用lambda表达式实现Runnable
       // new Thread( () -> System.out.println("In Java8, Lambda expression rocks !!") ).start();

        //3.使用lambda表达式和函数式接口Predicate
      /*  List<String> languages = Arrays.asList("Java", "Scala", "C++", "Haskell", "Lisp");

        System.out.println("Languages which starts with J :");

        System.out.println("Print all languages :");
        filter(languages, (str)->true);

        System.out.println("Print no language : ");
        filter(languages, (str)->false);*/

        //4.使用lambda表达式的Map和Reduce示例
        // 不使用lambda表达式为每个订单加上12%的税
      /*  List<Integer> costBeforeTax = Arrays.asList(100, 200, 300, 400, 500);
        for (Integer cost : costBeforeTax) {
            double price = cost + .12*cost;
            System.out.println(price);
        }*/

        // 使用lambda表达式
       // List<Integer> costBeforeTax = Arrays.asList(100, 200, 300, 400, 500);
       // costBeforeTax.stream().map((elmement) -> elmement + .12*elmement).forEach(System.out::println);

        // 为每个订单加上12%的税
       // 老方法：
     /*   List costBeforeTax = Arrays.asList(100, 200, 300, 400, 500);
        double total = 0;
        for (Object cost : costBeforeTax) {
            double price = (Integer)cost + .12*(Integer)cost;
            total = total + price;
        }
        System.out.println("Total : " + total);*/

        // 新方法：
       /* List<Integer> costBeforeTax = Arrays.asList(100, 200, 300, 400, 500);
        double bill = costBeforeTax.stream().map((cost) -> cost + .12*cost).reduce((sum, cost) -> sum + cost).get();
        System.out.println("Total : " + bill);*/

       //5.对列表的每个元素应用函数
        // 将字符串换成大写并用逗号链接起来
      /*  List<String> G7 = Arrays.asList("USA", "Japan", "France", "Germany", "Italy", "U.K.","Canada");
        String G7Countries = G7.stream().map(x -> x.toUpperCase()).collect(Collectors.joining(", "));
        System.out.println(G7Countries);*/

        //6.复制不同的值，创建一个子列表
        // 用所有不同的数字创建一个正方形列表
     /*   List<Integer> numbers = Arrays.asList(9, 10, 3, 4, 7, 3, 4);
        List<Integer> distinct = numbers.stream().map( i -> i*i).distinct().collect(Collectors.toList());
        System.out.printf("Original List : %s,  Square Without duplicates : %s %n", numbers, distinct);*/

        //7.计算集合元素的最大值、最小值、总和以及平均值
        List<Integer> primes = Arrays.asList(2, 3, 5, 7, 11, 13, 17, 19, 23, 29);
        IntSummaryStatistics stats = primes.stream().mapToInt((x) -> x).summaryStatistics();
        System.out.println("Highest prime number in List : " + stats.getMax());
        System.out.println("Lowest prime number in List : " + stats.getMin());
        System.out.println("Sum of all prime numbers : " + stats.getSum());
        System.out.println("Average of all prime numbers : " + stats.getAverage());
    }

/*    public static void filter(List<String> names, Predicate condition) {
        for(String name: names)  {
            if(condition.test(name)) {
                System.out.println(name + " ");
            }
        }
    }*/


    // 更好的办法
    public static void filter(List names, Predicate condition) {
        names.stream().filter((name) -> (condition.test(name))).forEach((name) -> {
            System.out.println(name + " ");
        });
    }
}
