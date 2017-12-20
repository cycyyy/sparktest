import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.UUID;

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

public class Main {
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static long THREE_HOURS = 3l * 3600l * 1000l;

    private static long ONE_HOUR = 3600l * 1000l;

    public static void main(String[] args) throws Exception {
        long time = System.currentTimeMillis();
        SparkConf conf = new SparkConf().setMaster("local").setAppName("test");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        JavaRDD<String> lines = ctx.textFile("src/main/resources/yellow_tripdata_2017-01.csv");
        JavaPairRDD<String, Iterable<Tuple2<String, Long>>> pickup = lines.mapToPair(new PairFunction<String, String, Tuple2<String, Long>>() {
            public Tuple2<String, Tuple2<String, Long>> call(String s) throws Exception {
                String[] datas = s.split(",");
                return new Tuple2<String, Tuple2<String, Long>>(datas[1].substring(0, 13),
                        new Tuple2<String, Long>(UUID.randomUUID().toString(), sdf.parse(datas[1]).getTime()));
            }
        }).groupByKey();

        JavaPairRDD<String, Iterable<Tuple2<String, Long>>> dropOff = lines.mapToPair(new PairFunction<String, String, Tuple2<String, Long>>() {
            public Tuple2<String, Tuple2<String, Long>> call(String s) throws Exception {
                String[] datas = s.split(",");
                return new Tuple2<String, Tuple2<String, Long>>(datas[2].substring(0, 13),
                        new Tuple2<String, Long>(UUID.randomUUID().toString(), sdf.parse(datas[2]).getTime()));
            }
        }).groupByKey().cache();
        JavaPairRDD<String, Tuple2<Iterable<Tuple2<String, Long>>, Iterable<Tuple2<String, Long>>>> result = null;
        for (int i = 0; i < 4; i++) {
            if (result != null)
                result = result.union(pickup.join(dropOff));
            else
                result = pickup.join(dropOff);
            if (i != 3)
                pickup = pickup.mapToPair(new AddOneHourFuncion());
        }
        result.flatMap(
                new FlatMapFunction<Tuple2<String, Tuple2<Iterable<Tuple2<String, Long>>, Iterable<Tuple2<String, Long>>>>, Iterable<Tuple2<Tuple2<String, String>, Tuple2<String, String>>>>() {

                    public Iterable<Iterable<Tuple2<Tuple2<String, String>, Tuple2<String, String>>>> call(
                            Tuple2<String, Tuple2<Iterable<Tuple2<String, Long>>, Iterable<Tuple2<String, Long>>>> stringTuple2Tuple2) throws Exception {
                        LinkedList result = new LinkedList();
                        Iterator<Tuple2<String, Long>> pickup = stringTuple2Tuple2._2()._1().iterator();
                        while (pickup.hasNext()) {
                            Tuple2<String, Long> pickupData = pickup.next();
                            Iterator<Tuple2<String, Long>> dropoff = stringTuple2Tuple2._2()._2().iterator();
                            while (dropoff.hasNext()) {
                                Tuple2<String, Long> dropoffData = dropoff.next();
                                if (!(dropoffData._2() - pickupData._2() > THREE_HOURS) && dropoffData._2() >= pickupData._2())
                                    result.add(new Tuple2<Tuple2<String, String>, Tuple2<String, String>>(
                                            new Tuple2<String, String>(pickupData._1(), sdf.format(new Date(pickupData._2()))),
                                            new Tuple2<String, String>(dropoffData._1(), sdf.format(new Date(dropoffData._2())))));
                            }
                        }
                        return result;
                    }
                }).saveAsTextFile("newResult");
        System.out.println(System.currentTimeMillis() - time);

    }

    public static class AddOneHourFuncion implements PairFunction<Tuple2<String, Iterable<Tuple2<String, Long>>>, String, Iterable<Tuple2<String, Long>>> {

        private static SimpleDateFormat sdfHour = new SimpleDateFormat("yyyy-MM-dd HH");

        public Tuple2<String, Iterable<Tuple2<String, Long>>> call(Tuple2<String, Iterable<Tuple2<String, Long>>> stringTuple2Tuple2) throws Exception {
            return new Tuple2<String, Iterable<Tuple2<String, Long>>>(returnOneHourLater(stringTuple2Tuple2._1()), stringTuple2Tuple2._2());
        }

        private String returnOneHourLater(String hour) throws ParseException {
            return sdfHour.format(new Date(sdfHour.parse(hour).getTime() + ONE_HOUR));
        }
    }
}
