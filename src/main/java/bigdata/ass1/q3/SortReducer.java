package bigdata.ass1.q3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SortReducer extends Reducer<Text, Text, Text, Text> {
    private Text outputValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text val : values) {
            outputValue.set(val);
            context.write(key, outputValue);
        }
    }
}
