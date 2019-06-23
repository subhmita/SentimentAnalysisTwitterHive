package com.hadoop.SentimentAnalysis;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.hadoop.SentimentAnalysis.Utils.SentimentAnalysisUtil;

public class SentimentAnalysisPreprocessingMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	JSONParser parser = new JSONParser();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		JSONObject json = null;
		try {
			json = (JSONObject) parser.parse(value.toString());
			JSONObject object = (JSONObject) json.get("quoted_status");
			String processedTweetText = null;
			String id = null;
			if (null != object && StringUtils.isNotBlank(String.valueOf(object))) {
				if (null != object.get("id") && null != object.get("text")
						&& StringUtils.isNotBlank(String.valueOf(object.get("id")))
						&& StringUtils.isNotBlank(String.valueOf(object.get("text")))) {
					id = String.valueOf(object.get("id")).trim();
					processedTweetText = SentimentAnalysisUtil
							.removeUrlHashAtTheRate(String.valueOf(object.get("text")));
					processedTweetText = SentimentAnalysisUtil.removeStopWords(processedTweetText);

				}
			} else {
				if (null != json.get("id") && null != json.get("text")
						&& StringUtils.isNotBlank(String.valueOf(json.get("id")))
						&& StringUtils.isNotBlank(String.valueOf(json.get("text")))) {
					id = String.valueOf(json.get("id")).trim();
					processedTweetText = SentimentAnalysisUtil.removeUrlHashAtTheRate(String.valueOf(json.get("text")));
					processedTweetText = SentimentAnalysisUtil.removeStopWords(processedTweetText);
				}
			}
			if (StringUtils.isNotBlank(id) && StringUtils.isNotBlank(processedTweetText)) {
				context.write(NullWritable.get(), new Text(id.concat(",").concat(processedTweetText)));
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
}
