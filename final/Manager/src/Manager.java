/**
 * 
 * 
 **/

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TagSpecification;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

public class Manager {
	public  AmazonEC2 ec2;
	public  AmazonS3 s3;
	public  AmazonSQS sqs;
	public  DefaultAWSCredentialsProviderChain credentialsProvider;
	public  int Num_Of_Workers ;
	public  final String Tasks_Queue = "tasksinput.fifo";
	public  final String Reviews_Queue = "reviewsinput.fifo";
	public String Tasks_Queue_url;
	public String Reviews_Queue_url;
	public ExecutorService tasks_executor;
	public boolean Running;
	public List<String> workers_id;
	public String instance_script;
	/*main metod :
	 * init mannger instance m 
	 * 	init its aws objects and variables 
	 * 	start it */
	public static void main(String args[]) throws IOException, InterruptedException {
		System.out.println("*      Manager Started        *");
		Manager m = new Manager();
         m.init_server();
         m.start();
	}
	public  void init_server()
    {
		System.out.println("init init_server .......................");
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
		
		final Map<String, String> attributes = new HashMap<String, String>();
	     workers_id = new ArrayList<String>();
		instance_script= this.getScript();
	 	// A FIFO queue must have the FifoQueue attribute set to True
	 	 attributes.put("FifoQueue", "true");
	 	attributes.put("ContentBasedDeduplication", "true");
	    CreateQueueRequest c_Q_R = new CreateQueueRequest(Tasks_Queue).withAttributes(attributes);
		Tasks_Queue_url = sqs.createQueue(c_Q_R).getQueueUrl();
		 c_Q_R = new CreateQueueRequest(Reviews_Queue).withAttributes(attributes);
		Reviews_Queue_url= sqs.createQueue(c_Q_R).getQueueUrl();
		tasks_executor = Executors.newFixedThreadPool(5);
		Running= true;
		
    	}
    	catch(Exception e)
    	{
    		System.out.println("exeption " + e.getMessage());
    	}
		System.out.println("......................done..........................");
    }
	/*start 
	 * start manager loop
	 * 	look for message from task queue 
	 * 		init runnable task handler and add it to executer 
	 *  repeat
	 *  after main loop stops 
	 *  terminate*/
	public void start ()
	{
		List<Message> recived_messages = new ArrayList<Message>()  ;
	     ReceiveMessageRequest getmessage ; 
	     Message  task;
		while (Running)
		{
			if(recived_messages.size() == 0)
			{
				getmessage = new ReceiveMessageRequest(Tasks_Queue_url)
						  .withWaitTimeSeconds(10).withMaxNumberOfMessages(1).withVisibilityTimeout(15);
				recived_messages=sqs.receiveMessage(getmessage).getMessages();
				System.out.println(recived_messages.size());
			}
			else {
		     task = recived_messages.remove(0);
		     System.out.println("task recived : " + task.getBody());
		     tasks_executor.execute(new ManagerTask(this, task.getBody()));
		     sqs.deleteMessage(new DeleteMessageRequest(Tasks_Queue_url,task.getReceiptHandle()));
		     recived_messages.clear();
			}
			
		}
		
		this.terminate();
		
	}
	/*terminate:
	 * wait and terminate all threads 
	 *  close all queues 
	 *  terminate  all workers instances that are running
	 *  shutdown @param ec2 object */
	public void terminate()
	{
		tasks_executor.shutdown();
		try {
		  tasks_executor.awaitTermination(Long.MAX_VALUE,java.util.concurrent.TimeUnit.NANOSECONDS );
		} catch (InterruptedException e) {
		  
		}
		   TerminateInstancesRequest tir = new TerminateInstancesRequest(workers_id);
		   if(workers_id.size()>0)
		   ec2.terminateInstances(tir); 
		   System.out.println(workers_id.size());
		sqs.deleteQueue(new DeleteQueueRequest(Tasks_Queue_url));
		sqs.deleteQueue(new DeleteQueueRequest(Reviews_Queue_url));
		ec2.shutdown();
	   System.out.println("..................................................manager terminated safely ");
	}
	/*lunch_worker...:
	 * get number of message in queue
	 * @param n workers: messages ratio
	 * calculate number of worker needed
	 * if needed workers init the appropriate amount if available
	 * lunch instances with userdata @param script
	 * add  new instances Id to @param worker_id to be terminated later  
	 * */
	public synchronized void lunch_workers_ifneeded(int n)
	{ 
		try {
		List<String> attributeNames = new ArrayList<String>();
		attributeNames.add("ApproximateNumberOfMessages");
		GetQueueAttributesRequest getatt = new GetQueueAttributesRequest(Reviews_Queue_url).withAttributeNames(attributeNames);
		Map<String, String> attributes=sqs.getQueueAttributes(getatt).getAttributes();
		int messages = Integer.parseInt(attributes
				.get("ApproximateNumberOfMessages"));
		int num_of_worker_to_lunch = messages/n;
		
		num_of_worker_to_lunch = num_of_worker_to_lunch -this.Num_Of_Workers;
		if(num_of_worker_to_lunch<1)
			return;
		System.out.println("number of worker to lunch : " +num_of_worker_to_lunch);
		TagSpecification t = new TagSpecification()
				.withResourceType("instance")
				.withTags(new Tag()
						.withKey("Name")
						.withValue("Worker"));
		RunInstancesRequest req;
		
			req = new RunInstancesRequest()
			.withImageId("ami-08d489468314a58df")
			.withInstanceType(InstanceType.T2Large)
			.withMinCount(1)
			.withMaxCount(num_of_worker_to_lunch)
			.withKeyName("VR2337.pem")
			.withUserData(instance_script)
			.withSecurityGroups("VR2337")
			.withTagSpecifications(t);
		
		RunInstancesResult result = ec2.runInstances(req);
		Thread.sleep(20000);
		 List<Instance> resultInstance = result.getReservation().getInstances();
         for (Instance ins : resultInstance){
        	 if(ins!=null)
         	workers_id.add(ins.getInstanceId());
         }
         Num_Of_Workers= Num_Of_Workers + resultInstance.size();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	//////////////////////////////////prepare script for worker instances ...
	public static String getScript() throws UnsupportedEncodingException {
		ArrayList<String> script = new ArrayList<String>();
		script.add("#!/usr/bin/env bash");
		script.add("export " + "AWS_ACCESS_KEY_ID=\"AKIAIETFEW77FTVUWUNQ\"");
		script.add("export " + "AWS_SECRET_ACCESS_KEY=\"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\"");
		script.add("wget https://manager15.s3-us-west-2.amazonaws.com/VR2337.pem");
		script.add("wget https://manager15.s3-us-west-2.amazonaws.com/Worker.jar");
		script.add("java -jar Worker.jar");
		String str = new String(Base64.encodeBase64(join(script, "\n").getBytes()), "UTF-8");
		return str;

	}
	private static String join(Collection<String> s, String delimiter) {
		StringBuilder builder = new StringBuilder();
		Iterator<String> iter = s.iterator();
		while (iter.hasNext()) {
			builder.append(iter.next());
			if (!iter.hasNext()) {
				break;
			}
			builder.append(delimiter);
		}
		return builder.toString();
	}
}
