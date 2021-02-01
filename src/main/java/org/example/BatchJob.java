/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import java.util.*;


/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */
public class BatchJob {

	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataSet<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/batch/index.html
		 *
		 * and the examples
		 *
		 * http://flink.apache.org/docs/latest/apis/batch/examples.html
		 *
		 */
	    
	    DataSet<String> text = env.fromElements("My First FLink Application");
	    
	    DataSet<Tuple2<String, Integer>> wordCounts = text
	    		 .flatMap(new LineSplitter())
	    		 .groupBy(0)
	    		 .sum(1);
	    
	    DataSet<Integer> num = env.fromCollection(Arrays.asList(10, 2, 22, 30, 41, 9, 12, 8));
	    DataSet<Integer> result = num.filter(new FilterFunction<Integer>() {
	    	public boolean filter(Integer values) {
	    		return values > 10;
	    	}
	    }).reduce((values, i) -> values + i);
	    
	    Tuple2<Integer, Integer> firstSet = new Tuple2<>(10, 20);
	    Tuple2<Integer, Integer> secondSet = new Tuple2<>(30, 40);
	    Tuple2<Integer, Integer> thirdSet = new Tuple2<>(50, 60);
	    DataSet<Tuple2<Integer, Integer>> numbers = env.fromElements(firstSet, secondSet, thirdSet);
	    DataSet<Integer> multiple = numbers.map(new Multiple());
		// execute program
//		wordCounts.print();
//		result.print();
		multiple.print();
	}
	
			public static class LineSplitter implements FlatMapFunction<String,
			Tuple2<String, Integer>> {
			 @Override
			 public void flatMap(String line, Collector<Tuple2<String, Integer>>
			out) {
				 for (String word : line.split(" ")) {
					 out.collect(new Tuple2<String, Integer>(word, 1));
				 }
			}
		}
			
			public static class Multiple implements MapFunction<Tuple2<Integer, Integer>, Integer> {
				@Override
				public Integer map(Tuple2<Integer, Integer> value) {
					return value.f0 * value.f1;
				}
			}

}

