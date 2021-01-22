package org.example.state;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.Collector;

public class UnderstandingValueState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = environment.socketTextStream("localhost",9090);

        DataStream<Long> res = data.map(new MapFunction<String, Tuple2<Long, String>>() {

            @Override
            public Tuple2<Long, String> map(String s) throws Exception {
                String[] d = s.split(",");
                return new Tuple2<>(Long.parseLong(d[0]),d[1]);
            }
        }).keyBy(0).flatMap(new StatefulMap());

        res.writeAsText("out/state");

        environment.execute("Value State");
    }

    public static class StatefulMap extends RichFlatMapFunction<Tuple2<Long,String>, Long>{

        private transient ValueState<Long> count;
        private transient ValueState<Long> sum;

        @Override
        public void flatMap(Tuple2<Long, String> input, Collector<Long> collector) throws Exception {
            Long currCnt = count.value();
            Long currSum = sum.value();

            currCnt +=1;
            currSum += Long.parseLong(input.f1);

            count.update(currCnt);
            sum.update(currSum);

            if(currCnt >=10){
                collector.collect(sum.value());

                sum.clear();
                count.clear();
            }
        }

        public void open(Configuration cfg){
//            ValueStateDescriptor<Long> countStateDesc = new ValueStateDescriptor<Long>("count", TypeInformation.of(new TypeHint<Long>(){}), 0L);
            count = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class, 0L));

//            ValueStateDescriptor<Long> sumStateDesc = new ValueStateDescriptor<Long>("count", TypeInformation.of(new TypeHint<Long>(){}), 0L);
            sum = getRuntimeContext().getState(new ValueStateDescriptor<Long>("sum", Long.class, 0L));
        }
    }
}
