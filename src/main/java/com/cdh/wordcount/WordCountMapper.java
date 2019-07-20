package com.cdh.wordcount;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
/**
 * mapper类继承Mapper  表示我们的这个class类是一个标准的mapper类，需要四个泛型
 * k1  v1   k2  v2
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
    Text text = new Text();
    IntWritable intWritable  =  new IntWritable();
    /**
     * 覆写父类的map方法，每一行数据要调用一次map方法，我们的处理逻辑都写在这个map方法里面
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     * hdfs的最原始数据
    hadoop,hive,hbase
    hive,storm
    hive,hbase,kafka
    spark,flume,kafka,storm
    hbase,hadoop,hbase
    hive,spark,storm
    经过第一步：TextInputFormat之后
    0  hadoop,hive,hbase
    17 hive,storm
    27 hive,hbase,kafka
     */
    /**
     * @param key  我们的key1   行偏移量 ，一般没啥用，直接可以丢掉
     * @param value  我们的value1   行文本内容，需要切割，然后转换成新的k2  v2  输出
     * @param context  上下文对象，承接上文，把数据传输给下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       /*
hadoop,hive,hbase
hive,storm
hive,hbase,kafka
spark,flume,kafka,storm
hbase,hadoop,hbase
hive,spark,storm
        */
        String line = value.toString();
        String[] split = line.split(",");
        //遍历我们切割出来的单词
        for (String word : split) {
            text.set(word);
            intWritable.set(1);
            //写出我们的k2  v2  这里的类型跟我们的k2  v2  保持一致
            context.write(text,intWritable);
        }
    }
}

