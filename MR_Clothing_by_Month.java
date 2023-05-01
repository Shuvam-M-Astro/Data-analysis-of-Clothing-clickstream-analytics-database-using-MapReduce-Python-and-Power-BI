import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GroupByMonth {

    public static class GroupByMonthMapper extends Mapper<Object, Text, Text, Text> {
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            
            // Skip header row
            if (line.startsWith("Clothing ID")) {
                return;
            }
            
            String[] parts = line.split(",");
            String monthStr = parts[7].substring(5, 7);
            int month = Integer.parseInt(monthStr);
            
            // Only process months 1 to 8
            if (month >= 1 && month <= 8) {
                context.write(new Text(monthStr), new Text(line));
            }
        }
    }
    
    public static class GroupByMonthReducer extends Reducer<Text, Text, NullWritable, Text> {
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> rows = new ArrayList<>();
            for (Text value : values) {
                rows.add(value.toString());
            }
            for (String row : rows) {
                context.write(NullWritable.get(), new Text(row));
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "GroupByMonth");
        job.setJarByClass(GroupByMonth.class);
        job.setMapperClass(GroupByMonthMapper.class);
        job.setReducerClass(GroupByMonthReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}