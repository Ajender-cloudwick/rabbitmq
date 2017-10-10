import java.util.*;

import com.stratio.receiver.RabbitMQUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import static org.apache.spark.api.java.StorageLevels.MEMORY_AND_DISK_SER_2;

/**
 * Created by AjenderNeelam on 10/6/17.
 */
public class RabbitMqEx {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Creating    Spark   Configuration");
        SparkConf conf = new SparkConf();
        conf.setAppName("RabbitMq Receiver Example");
        conf.setMaster("local[2]");

        System.out.println("Retreiving  Streaming   Context from    Spark   Conf");
        JavaStreamingContext streamCtx = new JavaStreamingContext(conf,
                Durations.seconds(2));

        Map<String, String>rabbitMqConParams = new HashMap<String, String>();
        rabbitMqConParams.put("host", "localhost");
        rabbitMqConParams.put("queueName", "hello1");
        System.out.println("Trying to connect to RabbitMq");
        JavaReceiverInputDStream<String> receiverStream = RabbitMQUtils.createJavaStreamFromAQueue(streamCtx,"localhost",4369,"hello1",MEMORY_AND_DISK_SER_2);
        KafkaUtils.createStream(streamCtx,"localhost","tst1",);
        System.out.println("Value Received " + receiverStream );
               // createJavaStream(streamCtx, rabbitMqConParams);
        //receiverStream.foreachRDD();
       /*receiverStream.foreachRDD(new Function<JavaRDD<String>, Void>() {
            @Override
            public Void call(JavaRDD<String> arg0) throws Exception {
                System.out.println("Value Received " + arg0.toString());
                return null;
            }
        } );*/
        //receiverStream.foreachRDD();
        //System.out.println();

        System.out.println(receiverStream.count());
        receiverStream.count();
       // receiverStream
        streamCtx.start();
        streamCtx.awaitTermination();
    }
}
