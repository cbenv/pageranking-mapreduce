package edu.neu.mapreduce.project;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class MetadataParser {

    public static class OutlinksMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

        private final String COUNTER_GROUP = "Custom Mapper Counters";

        public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            String url = key.toString();
            String json = value.toString();

            try {
                JsonParser jsonParser = new JsonParser();
                JsonObject jsonObj = jsonParser.parse(json).getAsJsonObject();

                String disposition;

                if (jsonObj.has("disposition")) {
                    disposition = jsonObj.get("disposition").getAsString().trim().toUpperCase();
                    if (disposition.equals("SUCCESS") && jsonObj.has("content")) {
                        JsonObject content = (JsonObject) jsonObj.get("content");
                        if (content.has("type")) {
                            if ("html-doc".equalsIgnoreCase(content.get("type").getAsString().trim()
                                    .toLowerCase())) {
                                StringBuilder outLinks = new StringBuilder();
                                if (content.has("links")) {
                                    JsonArray allLinks = content.getAsJsonArray("links");
                                    for (int i = 0; i < allLinks.size(); i++) {
                                        JsonObject obj = allLinks.get(i).getAsJsonObject();
                                        if (obj.has("href")) {
                                            String href = obj.get("href").getAsString().trim();
                                            href = org.apache.commons.lang3.StringEscapeUtils.escapeJava(href);
                                            outLinks.append(href).append(",");
                                        }
                                    }
                                }
                                output.collect(new Text(url), new Text(outLinks.toString()));
                            }
                        }
                    }
                }
            } catch (Exception ex) {
                reporter.incrCounter(this.COUNTER_GROUP, "Exceptions", 1);
            }
        }
    }

    public static class OutlinksReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String pagerank = "1.0\t";
            boolean first = true;
            while (values.hasNext()) {
                if (!first) pagerank += ",";
                pagerank += values.next().toString();
                first = false;
            }
            output.collect(key, new Text(pagerank));
        }
    }
}
