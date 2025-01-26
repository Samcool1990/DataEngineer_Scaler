package com.hadoop.logger;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	private IntWritable hour = new IntWritable();
	private final static IntWritable one = new IntWritable(1);
	private static Pattern logPattern = Pattern
			.compile("([^ ]*) ([^ ]*) ([^ ]*) \\[([^]]*)\\]" + " \"([^\"]*)\"" + " ([^ ]*) ([^ ]*).*");
	private static SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");

	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
		
		Date date = null;
		String line = ((Text) value).toString();
		Matcher matcher = logPattern.matcher(line);
		if (matcher.matches()) {
			String timestamp = matcher.group(4);
			try {
				date = (Date) sdf.parse(timestamp);
			} catch (ParseException ex) {
				ex.printStackTrace();
			}
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			hour.set(cal.get(Calendar.HOUR_OF_DAY));
			context.write(hour, one);
		}
	}
}
