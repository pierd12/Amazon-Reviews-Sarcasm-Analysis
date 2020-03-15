import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
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
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.apache.commons.codec.binary.Base64;

import java.io.*;
import java.util.*;

public class ManagerTask implements Runnable{
 public Manager m ;
 public int numofworkers;
 public User user;
 public String user_bucket;
 public String file_num;
 /*constructor:
  * manager @param m construct manger task with @param message
  * if terminate messages is received shutdown @param m 
  * save data in task members*/
 public ManagerTask(Manager m , String message) {
	this.m=m;
	String in [] = message.split(",");
	user_bucket= in[0];
	numofworkers = Integer.parseInt(in[1]);
	if(in[2].toLowerCase().contentEquals("true"))
		m.Running = false;
	file_num= in[3];
	System.out.println("message recived from  : " + in[0]);
}
@Override
/*run tast
 *  init User that task belongs to
 *  push user reviews in Reviews queue so worker can read them
 *  lunch workers if need to handle reviews
 *  wait for reuslt from workers and collect it 
 *  write result to s3   */
public void run() {
   init_user();
   push_reviews();
   m.lunch_workers_ifneeded(numofworkers);
   collect_result();
   write_summry();
	
}
/*init_user
 *  init user and saved in user variable  
 *  parse files from s3 and create array of reviews objects*/
 private void init_user()
 {
	 user=new User(user_bucket);
	 user.parse_reviews(m.s3);
 }
 /*push_review :
  * create queue that worker will push result in it 
  * for each review make message suits it and push message in Reviews queue
  * */
 private void  push_reviews()
 { 
	 String message;
	 final Map<String, String> attributes = new HashMap<String, String>();

 	// A FIFO queue must have the FifoQueue attribute set to True
 	 attributes.put("FifoQueue", "true");
 	attributes.put("ContentBasedDeduplication", "true");
 	CreateQueueRequest c_Q_R = new CreateQueueRequest(user_bucket+".fifo").withAttributes(attributes);
 	String queue_url= m.sqs.createQueue(c_Q_R).getQueueUrl();
		 try {
			Thread.sleep(5);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	 for (Review r  : user.reviews)
	 {
      message = queue_url+",,"+r.toString()+",," + file_num;
	 m.sqs.sendMessage(new SendMessageRequest(m.Reviews_Queue_url , message).withMessageGroupId(r.getId()+file_num));
	 }
 }
 /*collect result : 
  *   get output queue url 
  *   wait for messages represent workers result
  *   loop 
  *   receive message and it to list of result 
  *   repeat tel we have the results
  *   clean up result queue */
 @SuppressWarnings("static-access")
private void collect_result()
 {
	 List<Message> res = new ArrayList<Message>(); 
	 ReceiveMessageRequest receiveMessageRequest;
	 String queue_url = m.sqs.getQueueUrl(new GetQueueUrlRequest(user_bucket+".fifo")).getQueueUrl();
	 while (user.reviews.size()!=user.results.size())
	 {
		
		 receiveMessageRequest = new ReceiveMessageRequest(queue_url)
				  .withWaitTimeSeconds(10)
				  .withMaxNumberOfMessages(5);
		 res = m.sqs.receiveMessage(receiveMessageRequest).getMessages();
		 for (Message sqs_m : res) {
			 user.results.add(sqs_m.getBody());
			 m.sqs.deleteMessage(new DeleteMessageRequest()
					  .withQueueUrl(user_bucket+".fifo")
					  .withReceiptHandle(sqs_m.getReceiptHandle()));
		 }
		 res.clear();
		  
	 }
	 try {
		Thread.sleep(10);
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	 receiveMessageRequest = new ReceiveMessageRequest(queue_url)
			  .withWaitTimeSeconds(10)
			  .withMaxNumberOfMessages(10);
	 res = m.sqs.receiveMessage(receiveMessageRequest).getMessages();
	 for (Message sqs_m : res) {
		 user.results.add(sqs_m.getBody());
		 m.sqs.deleteMessage(new DeleteMessageRequest()
				  .withQueueUrl(user_bucket+".fifo")
				  .withReceiptHandle(sqs_m.getReceiptHandle()));
	 }
	
	 
 }
 /*write_summary:
  *  for each result 
  *   print line result in output file in s3 
  *  send message to local indicating output file is ready 
  *  */
 private void write_summry()
 {
	String url ;
	try {
		File output = new File("output");
		 OutputStream instream;
		instream = new FileOutputStream(output);
		 PrintWriter writer = new PrintWriter(instream);
		 for (String line : user.results)
			 writer.println(line);
		 writer.close();
	 m.s3.putObject(new PutObjectRequest(user_bucket, "outputFile", output));
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}               
   url =  m.sqs.createQueue(new CreateQueueRequest(user_bucket+"out")).getQueueUrl();
   m.sqs.sendMessage(new SendMessageRequest().withQueueUrl(url).withMessageBody("done"));   /////////////telling the user we finish.
 }
}
