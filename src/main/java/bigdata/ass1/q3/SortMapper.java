package bigdata.ass1.q3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SortMapper extends Mapper<Object, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // we assume key-value pairs are in the form "key:value key:value"

        String[] pairs = value.toString().split("\\s+");  // split by whitespace
        for (String pair : pairs) {
            String[] kv = pair.split(":");
            if (kv.length == 2) {
                outputKey.set(kv[0]);
                outputValue.set(kv[1]);
                context.write(outputKey, outputValue);  // emit the key-value pair
            }
        }
    }
}