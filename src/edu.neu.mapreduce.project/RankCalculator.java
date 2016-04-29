package edu.neu.mapreduce.project;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class RankCalculator {

    public static class RankMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            if (value.toString().startsWith("http")) {
                int pageTabIndex = value.find("\t");
                int rankTabIndex = value.find("\t", pageTabIndex + 1);

                String page = Text.decode(value.getBytes(), 0, pageTabIndex);
                String pageWithRank = Text.decode(value.getBytes(), 0, rankTabIndex + 1);

                // Mark page as an Existing page (ignore red wiki-links)
                output.collect(new Text(page), new Text("!"));

                // Skip pages with no links.
                if (rankTabIndex == -1) return;

                String links = Text.decode(value.getBytes(), rankTabIndex + 1, value.getLength() - (rankTabIndex + 1));
                String[] allOtherPages = links.split(",");
                int totalLinks = Math.max(allOtherPages.length - 1, 0);

                for (String otherPage : allOtherPages) {
                    Text pageRankTotalLinks = new Text(pageWithRank + totalLinks);
                    output.collect(new Text(otherPage), pageRankTotalLinks);
                }

                // Put the original links of the page for the reduce output
                output.collect(new Text(page), new Text("|" + links));
            }
        }
    }

    public static class RankReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        private static final float damping = 0.85F;

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
            boolean isExistingWikiPage = false;
            String[] split;
            float sumShareOtherPageRanks = 0;
            String links = "";
            String pageWithRank;

            // For each otherPage:
            // - check control characters
            // - calculate pageRank share <rank> / count(<links>)
            // - add the share to sumShareOtherPageRanks

            while (values.hasNext()) {
                pageWithRank = values.next().toString();

                if (pageWithRank.equals("!")) {
                    isExistingWikiPage = true;
                    continue;
                }

                if (pageWithRank.startsWith("|")) {
                    links = "\t" + pageWithRank.substring(1);
                    continue;
                }

                split = pageWithRank.split("\\t");

                float pageRank = Float.valueOf(split[1]);
                int countOutLinks = Integer.valueOf(split[2]);

                sumShareOtherPageRanks += (pageRank / countOutLinks);
            }

            if (!isExistingWikiPage) return;
            float newRank = damping * sumShareOtherPageRanks + (1 - damping);

            out.collect(key, new Text(newRank + links));
        }
    }

}
