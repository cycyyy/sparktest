import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import java.text.SimpleDateFormat;

import scala.Tuple2;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class MainOld {
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static Long THREE_HOUR = 3l * 3600l * 1000l;

    public static void main(String[] args) throws Exception {
        long time = System.currentTimeMillis();
        SparkConf conf = new SparkConf().setMaster("local").setAppName("test");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        JavaRDD<String> lines = ctx.textFile("src/main/resources/yellow_tripdata_2017-01.csv");
        JavaPairRDD<Long, String> pickUp = lines.mapToPair(new PairFunction<String, Long, String>() {
            public Tuple2<Long, String> call(String s) throws Exception {
                String[] datas = s.split(",");
                return new Tuple2<Long, String>(sdf.parse(datas[1]).getTime(), datas[1]);
            }
        });

        JavaPairRDD<Long, String> dropOff = lines.mapToPair(new PairFunction<String, Long, String>() {
            public Tuple2<Long, String> call(String s) throws Exception {
                String[] datas = s.split(",");
                return new Tuple2<Long, String>(sdf.parse(datas[2]).getTime(), datas[2]);
            }
        });
        pickUp.cartesian(dropOff).filter(new Function<Tuple2<Tuple2<Long, String>, Tuple2<Long, String>>, Boolean>() {
            public Boolean call(Tuple2<Tuple2<Long, String>, Tuple2<Long, String>> tuple2Tuple2Tuple2) throws Exception {
                if ((tuple2Tuple2Tuple2._2()._1() - tuple2Tuple2Tuple2._1()._1()) > THREE_HOUR)
                    return false;
                if ((tuple2Tuple2Tuple2._2()._1() < tuple2Tuple2Tuple2._1()._1()))
                    return false;
                return true;
            }
        }).saveAsTextFile("result");
        System.out.println(System.currentTimeMillis() - time);

    }
}

