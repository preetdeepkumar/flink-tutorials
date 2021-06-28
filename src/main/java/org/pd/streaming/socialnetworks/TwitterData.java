package org.pd.streaming.socialnetworks;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.Random;

public class TwitterData
{
    public static void main(String[] args) throws Exception
    {
        final StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();

        Properties twitterCredentials = new Properties();
        // Provide your credentials! These credentials are not working. Do not this in production, please.
        twitterCredentials.setProperty(TwitterSource.CONSUMER_KEY, "AJcUxpK8yRKE5yzZyteyVsUOk");
        twitterCredentials.setProperty(TwitterSource.CONSUMER_SECRET, "8XzIDXlVLQANLsItrALwx3dHtyVvq4vrZu2OYSfhEqFsSEKI12");
        twitterCredentials.setProperty(TwitterSource.TOKEN, "183234343-U6hOdLRXvWlDTkwaQxBRGEZjMa9sxbGAMGZ5ZfSL");
        twitterCredentials.setProperty(TwitterSource.TOKEN_SECRET, "S5m6OEZ7q5QuVtgJV52SjhUaaV1Yna5QfQSOqt47Qt7Ze");

        DataStream<String> twitterData = env.addSource(new TwitterSource(twitterCredentials));

        Random random = new Random();
        String path = "file:///C://Users//ai.roman//tweet-" + random.nextInt();
        twitterData.flatMap(new TweetParser()).writeAsText(path);

        env.executeAsync("Twitter Example");
        System.out.print("Done!. A file was created at " + path);
    }

    public static class TweetParser	implements FlatMapFunction<String, Tuple2<String, Integer>>
    {

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception
        {
            ObjectMapper jsonParser = new ObjectMapper();
            JsonNode node = jsonParser.readValue(value, JsonNode.class);

            boolean isEnglish =
                    node.has("user") &&
                            node.get("user").has("lang") &&
                            node.get("user").get("lang").asText().equals("en");

            boolean hasText = node.has("text");

            if (isEnglish && hasText)
            {
                String tweet = node.get("text").asText();

                out.collect(new Tuple2<String, Integer>(tweet, 1));
            }
        }
    }
}

