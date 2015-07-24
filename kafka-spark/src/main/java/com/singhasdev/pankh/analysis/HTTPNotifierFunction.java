package com.singhasdev.pankh.analysis;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple5;
import scala.Tuple6;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

public class HTTPNotifierFunction
    implements Function<JavaRDD<Tuple6<Long, String, String, Float, Float, String>>,
                        Void>
{
    private static final long serialVersionUID = 42l;

    @Override
    public Void call(JavaRDD<Tuple6<Long, String, String, Float, Float, String>> rdd)
    {
        rdd.foreach(new SendPostFunction());
        return null;
    }
}

class SendPostFunction
    implements VoidFunction<Tuple6<Long, String, String, Float, Float, String>>
{
    private static final long serialVersionUID = 42l;

    public void call(Tuple6<Long, String, String, Float, Float, String> tweet)
    {
    
    /*String confFile;
    if (args.length != 1) {
      //logger.warn("A config file is expected as argument. Using default file, conf/analyzer.conf");
      confFile = "conf/analyzer.conf";
    } else {
      confFile = args[0];
    }*/
    
    String confFile;
    confFile = "conf/analyzer.conf";
    Context context;
    try {
      context = new Context(confFile);
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
        
        //String webserver = context.getString("rts.spark.webserv");
        String webserver="http://localhost:3000/post";
        HttpClient client = new DefaultHttpClient();
        HttpPost post = new HttpPost(webserver);
        String content = String.format(
            "{\"id\": \"%d\", "     +
            "\"text\": \"%s\", "    +
            "\"pos\": \"%s\", "     +
            "\"neg\": \"%s\", "     +
            "\"score\": \"%s\" }",
            tweet._1(),
            tweet._2(),
            tweet._4(),
            tweet._5(),
            tweet._6());

        try
        {
            post.setEntity(new StringEntity(content));
            HttpResponse response = client.execute(post);
            org.apache.http.util.EntityUtils.consume(response.getEntity());
        }
        catch (Exception ex)
        {
            Logger LOG = Logger.getLogger(this.getClass());
            LOG.error("exception thrown while attempting to post", ex);
            LOG.trace(null, ex);
        }
    }
}
