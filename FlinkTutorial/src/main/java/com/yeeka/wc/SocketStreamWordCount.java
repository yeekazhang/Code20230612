package com.yeeka.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class SocketStreamWordCount {
    public static void main(String[] args) throws Exception {

        // 1 创建流式执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // IDEA运行时，也可以看到UI，一般用于本地测试
        // 需要引入一个依赖
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 2 读取文本流：hadoop102表示发送主机名、7777表示端口号
        DataStreamSource<String> lineStream = env.socketTextStream("hadoop102", 7777);


        // 在idea运行，不指定并行度，默认就是 电脑的 线程数
        env.setParallelism(1);

        // 全局禁用算子链
//        env.disableOperatorChaining();


        // 3 转换、分组、求和，得到统计结果
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = lineStream.flatMap(
                        (String value, Collector<String> out) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(word);
                            }
                        })
                .startNewChain()
//                .disableChaining()
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(value -> value.f0)
                .sum(1);

        // 4 打印
        sum.print();

        // 5 执行
        env.execute();

    }
}










