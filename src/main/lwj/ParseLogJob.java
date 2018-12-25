package lwj;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class ParseLogJob extends Configured implements Tool {
    public static JSONObject parseLog(String row) throws ParseException {
        String[] parts = StringUtils.split(row, "\u1111");

        JSONObject logData = JSON.parseObject(parts[2]);
        logData.put("active_name", parts[1]);
        logData.put("time_tag", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(parts[0]).getTime());

        return logData;
    }

    public static class LogMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        private MultipleOutputs outputs;

        public void setup(Context context) {
            outputs = new MultipleOutputs(context);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                Text parsedLog = new Text(parseLog(value.toString()).toJSONString());
                context.write(new LongWritable(1), parsedLog);
                outputs.write("parsed", null, parsedLog);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class LogReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
        private MultipleOutputs outputs;

        private Map<String, Boolean> users = new HashMap<String, Boolean>();
        private long count_records = 0;
        private long count_users = 0;

        public void setup(Context context) {
            outputs = new MultipleOutputs(context);
        }

        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                JSONObject parsedLog = JSON.parseObject(value.toString());
                if (!users.containsKey(parsedLog.getString("user_id"))) {
                    users.put(parsedLog.getString("user_id"), true);
                    count_users++;
                }
                count_records++;
            }

            context.getCounter("Log Counter", "Count Records").setValue(count_records);
            context.getCounter("Log Counter", "Count Users").setValue(count_users);

            JSONObject counts = new JSONObject();
            counts.put("count_records", count_records);
            counts.put("count_users", count_users);

            outputs.write("counts", null, new Text(counts.toJSONString()));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration config = getConf();

        Job job = Job.getInstance(config);
        job.setJobName("parseLog");
        job.setJarByClass(ParseLogJob.class);
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        MultipleOutputs.addNamedOutput(job, "parsed", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "counts", TextOutputFormat.class, NullWritable.class, Text.class);

        FileSystem fs = FileSystem.get(config);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        if (!job.waitForCompletion(true)) {
            throw new RuntimeException(job.getJobName() + " failed");
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new ParseLogJob(), args));
    }
}