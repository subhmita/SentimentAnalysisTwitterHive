package com.hadoop.SentimentAnalysis.Utils;

import org.apache.commons.lang.StringUtils;
public class SentimentAnalysisUtil {
	public static String removeUrlHashAtTheRate(String text) {
	    return text.replaceAll("http.*?://\\S+\\s?", "")
	    		.replaceAll("[^a-zA-Z\\s]", "")
	    		.replaceAll("\\s+", " ")
	    		.trim();
	}
	public static String removeStopWords(String text) {
	    return text.replaceAll("(\\b" + StringUtils.join(StopWords.STOP_WORDS, "\\b|\\b") + "\\b)", "").replaceAll("\\s+"," ")
	    		.trim();
	}
	
}
