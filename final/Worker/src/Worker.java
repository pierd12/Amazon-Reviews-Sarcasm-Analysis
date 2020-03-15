/**
 * 
 * 
 **/
import java.io.BufferedReader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Principal;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.auth.policy.resources.S3ObjectResource;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TagSpecification;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.org.apache.xpath.internal.operations.Bool;
import java.util.List;
import java.util.Properties;
 
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
public class Worker {
	public  AmazonEC2 ec2;
	public  AmazonS3 s3;
	public  AmazonSQS sqs;
	public  DefaultAWSCredentialsProviderChain credentialsProvider;
	public StanfordCoreNLP  sentimentPipeline;
	public StanfordCoreNLP NERPipeline;
	public  final String Reviews_Queue = "reviewsinput.fifo";
	public String Reviews_Queue_url;
	public static void main(String arg[]) {
		System.out.println("*      Worker Started        *");
		Worker w = new Worker();
		w.work();
		
		
	}
	/*constructor 
	 * init object variables 
	 * init parsing tools */
	public Worker()
	{
		init_server();
		Properties props = new Properties();
		props.put("annotators", "tokenize, ssplit, parse, sentiment");
		sentimentPipeline =  new StanfordCoreNLP(props);
		 props = new Properties();
		props.put("annotators", "tokenize , ssplit, pos, lemma, ner");
		NERPipeline =  new StanfordCoreNLP(props);
	}
	/*work :
	 * start infinite  loop 
	 *  look for reviews from reviews_queue
	 *   for each review 
	 *    work on reviews with sentiment and Entities check if sarcastic
	 *    get result queue url from message received  
	 *    send result and delete review from queue
	 *    repeat
	 *   repeat   
	 * */
	public void work()
	{
		Reviews_Queue_url = sqs.getQueueUrl(new GetQueueUrlRequest(Reviews_Queue)).getQueueUrl();
		//String Colors [] = {"dark red" , "red","black","light green","dark green"};
		String queue_name;
		String url;
		List<Message> message ;
		String sarcastic  = "sarcastic";
		String not_sarcastic = "not sarcastic";
		Gson g = new GsonBuilder()
		        .setLenient()
		        .create();
		String [] m ;
		Review r;
		String res;
		int sentment ;
		
		while (true)
		{
			try {
			message = sqs.receiveMessage(new ReceiveMessageRequest().withQueueUrl(Reviews_Queue_url).withMaxNumberOfMessages(10).withWaitTimeSeconds(5).withVisibilityTimeout(150)).getMessages();
			if(message.size() == 0)
				continue;
		for(Message in : message) { 	
			m = in.getBody().split(",,");
			//System.out.println(m[0]);
			//System.out.println(m[1]);
			queue_name = m[0];
			r= g.fromJson(m[1], Review.class);
			sentment = this.findSentiment(r.getText());
			if(sentment>r.getRating())
				res = sentment+",," + m[1]+",,"+ printEntities(r.getText())+ ",,"+sarcastic +",,"+m[2];
			else 
					res = sentment+",,"+m[1]+",,"+ printEntities(r.getText())+ ",,"+not_sarcastic+",,"+m[2];
		//url = sqs.getQueueUrl(new GetQueueUrlRequest(queue_name)).getQueueUrl();
		sqs.sendMessage(new SendMessageRequest(queue_name, res).withMessageGroupId(r.getId()+m[2]));	
		sqs.deleteMessage(new DeleteMessageRequest(Reviews_Queue_url, in.getReceiptHandle()));
		}
		message.clear();
		}catch(AmazonServiceException  aw)
			{
			  System.out.println(aw.getErrorMessage());
			  break;
			}
		}
		
		
		
		
	}
    
	public  void init_server()
    {
		System.out.println("init Worker  .......................");
    	try {
    		credentialsProvider = new DefaultAWSCredentialsProviderChain();
    	
		ec2 = AmazonEC2ClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-west-2")
				.build();

		s3 = AmazonS3ClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-west-2")
				.build();

		sqs = AmazonSQSClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-west-2")
				.build();
    	}
    	catch(Exception e)
    	{
    		System.out.println("exeption " + e.getMessage());
    	}
   }
	public int findSentiment(String review) {
		 
        int mainSentiment = 0;
        if (review!= null && review.length() > 0) {
            int longest = 0;
            Annotation annotation = sentimentPipeline.process(review);
            for (CoreMap sentence : annotation
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence
                        .get(SentimentCoreAnnotations.AnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }
 
            }
        }
        return mainSentiment;
   }
	public  String  printEntities(String review){
        // create an empty Annotation just with the given text
        Annotation document = new Annotation(review);
        StringBuilder ans = new StringBuilder();
 
        // run all Annotators on this text
        NERPipeline.annotate(document);
 
        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
        ans.append("[ ");
 
        for(CoreMap sentence: sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(NamedEntityTagAnnotation.class);
                ans.append( word + ": " + ne + " , ");
            }
        }
        ans.deleteCharAt(ans.length()-1);
        ans.append(" ]");
       return ans.toString();
    }
}
