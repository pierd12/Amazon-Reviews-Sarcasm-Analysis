/**
 * 
 * 
 **/


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.Request;
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
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;

import org.apache.commons.codec.binary.Base64;
import java.io.*;
import java.util.*;



/**
 * Hello world!
 *
 */
public class App 
{
	public static AmazonEC2 ec2;
	public static DefaultAWSCredentialsProviderChain credentialsProvider;
	public static AmazonS3 s3;
	public static AmazonSQS sqs;
	static boolean Terminate = false;
	public static int n;
	public static final String Tasks_Queue = "tasksinput.fifo";
	public static  File [] input_files ;
	public  static String bucket_name;
	public static File [] output_files;
	public static int  current_file ;
	public static String output_queue_url;
	public static String tasks_queue_url;
	/*main method :
	 *   init aws objects 
	 *  recive arguments from user 
	 * init local application bucket to upload files in s3
	 *   start connecting to manager   */
    public static void main( String[] args )
    {
    	 init_server();

    	 extract_args(args);
    	 init_bucket();
    	 start();
    	 
    	
    	    
    }
    /*extract_....:
     * parse arguments from terminal
     * initlocal application public  variables and  files */
    public static void  extract_args(String []args)
    {
    	System.out.println("Reading local arguments ....................");
    	int len ;
    	if(args.length==0) {
    		System.err.println("...................No Args.............");
    		throw new RuntimeException();
    		
    	}
    	if(args[args.length-1].equals("terminate"))
    	{
    		Terminate = true;
    		n = Integer.parseInt(args[args.length-2]);
    		len =(args.length -2)/2;
    		
    	}
    	else {
    		len =(args.length -1)/2;
    	n = Integer.parseInt(args[args.length-1]);
    	}
    	input_files = new File[len];
    	output_files = new File[len];
    	for (int i = 0 ; i < len;i++) {
    		input_files[i] = new File(args[i]);
    		output_files[i] = new File(args[len + i]);
    	}
    	
    	System.out.print("args : n: "+ n+" Terminate :  ");
    	System.out.print(Terminate);
    	System.out.println("  num of input files  : "+ input_files.length);
    	System.out.println(".................done...............");
    		
    	
    }
    /*init-server :
     * init @param credentials from env keys 
     * init ec2 ,sqs ,s3 objects from @param credentials
     * init task_queue and save its url in public variable  */
	@SuppressWarnings("static-access")
    public static void init_server()
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

    	// A FIFO queue must have the FifoQueue attribute set to True
    	 attributes.put("FifoQueue", "true");
    	attributes.put("ContentBasedDeduplication", "true");
    	CreateQueueRequest c_Q_R = new CreateQueueRequest(Tasks_Queue).withAttributes(attributes);
		 tasks_queue_url = sqs.createQueue(c_Q_R).getQueueUrl();
    	}
    	
    	catch(Exception e)
    	{
    		System.out.println("exeption " + e.getMessage());
    	}
		System.out.println("......................done..........................");
    }
	/*init_bucket :
	 * init @param bucket name with random number and user_name
	 * init bucket in s3 with @param bucket_name name and set it permission to allow public use
	 * init public variable bucket_name*/
    public static void init_bucket()
    {
    	System.out.println("init bucket .......................");
    	Random r = new Random();
    	bucket_name = System.getProperty("user.name") + Integer.toString(r.nextInt(100));
    	System.out.println("bucket_name:"+bucket_name);
    	try {
			if (!s3.doesBucketExistV2(bucket_name)) {
				System.out.println("create new bucket");
				s3.createBucket(new CreateBucketRequest(bucket_name));
				Statement allowPulbicStatment = new Statement(Effect.Allow)
						.withPrincipals(Principal.AllUsers)
						.withActions(S3Actions.GetObject)
						.withResources(new S3ObjectResource(bucket_name, "*"));
				Policy policy = new Policy()
						.withStatements(allowPulbicStatment);
				s3.setBucketPolicy(bucket_name, policy.toJson());
				
				//access control
				 AccessControlList acl = s3.getBucketAcl(bucket_name);
				 acl.grantPermission(GroupGrantee.AllUsers, Permission.FullControl);
				 acl.grantPermission(GroupGrantee.AllUsers, Permission.Read);
				 acl.grantPermission(GroupGrantee.AllUsers, Permission.Write);
			}
			else System.out.println("why bucket already there wtf");
			}
    	catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");

		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
    	System.out.println("......................done..........................");
    }
    /*start :
     * init queue for manager output messages @param output_queue_url
     * check the mangger and luch it if needed
     * for each file @param current_file in @param input_files:
     *    upload file 
     *    send message to maneger to analyze it
     *    wiat for result 
     *    write suitable html file in @param output_files @param curret_file
     *    increase @param current_file and repeat
     * after all finished terminate queues and bucket for local user     */
    public static void start()
    {
    	String task_message ;
    	current_file = 0;
    	CreateQueueRequest c_Q_R = new CreateQueueRequest(bucket_name+"out");
		 output_queue_url = sqs.createQueue(c_Q_R).getQueueUrl();
		checkManagerAndLaunch();
    	
    	try {
    		while (current_file<input_files.length)
    		{
			uploadfile();
    		if(current_file == input_files.length-1)
			task_message = prepare_msg(Terminate);
    		else
    		task_message = prepare_msg(false);
			openq_sendmssg(task_message);
			wait_manager_to_finish();
			downloadfile_tohtm();
			
			System.out.println(output_files [current_file].getName()  +" is ready");
			  current_file++;
    		    }
    		
             
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	delete_bucket_queue();
    	return;
    }
    /*wait_... :
     * wait done message form manager indicates he is done */
    public static void wait_manager_to_finish() {
		System.out.println("*********************) Waiting Manager to finish: ");
		List<Message> l;
		while ((l = sqs.receiveMessage(new ReceiveMessageRequest().withQueueUrl(output_queue_url).withWaitTimeSeconds(10)).getMessages()).size() == 0) {
			try {
				Thread.sleep(5);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
           sqs.deleteMessage(new DeleteMessageRequest().withQueueUrl(output_queue_url).withReceiptHandle(l.get(0).getReceiptHandle()));
	}
	/*delete_bukcet ... : 
	 * delete bucket with bucket name @param bucket_name and all files in it 
	 * delete all queues that local user initiated or manager for this local application*/
    public static void delete_bucket_queue()
    {
    	ObjectListing objectListing = s3.listObjects(bucket_name);
            Iterator<S3ObjectSummary> objIter = objectListing.getObjectSummaries().iterator();
            while (objIter.hasNext()) {
                s3.deleteObject(bucket_name, objIter.next().getKey());
            }	
          sqs.deleteQueue(new DeleteQueueRequest(output_queue_url));
        s3.deleteBucket(bucket_name);
        String queue_url = sqs.getQueueUrl(new GetQueueUrlRequest(bucket_name+".fifo")).getQueueUrl();
        sqs.deleteQueue(new DeleteQueueRequest(queue_url));
        System.out.println("...............................................................................local app terminated");
    } 
    /*downfile_.. :
     * down laod from s3 and write html file in @param outputfiles thats suits the reviews*/
    public static void downloadfile_tohtm()
    {
    	String Colors [] = {" Maroon" , "red","black","LimeGreen","darkgreen"};
    	
    	try {
    	    S3Object o = s3.getObject(new GetObjectRequest(bucket_name, "outputFile"));
    	    InputStream s3is = o.getObjectContent();
    	    FileOutputStream fos = new FileOutputStream(output_files[current_file]);
    	    BufferedReader reader = new BufferedReader(
                    new InputStreamReader(s3is));
    	    PrintWriter writer = new PrintWriter(fos);
    	    writer.println("<!DOCTYPE html>");
    	    writer.println("<html>");
    	    writer.println("<body>");
    	    String read ;
    	    String [] towrite ;
    	    while ((read = reader.readLine()) !=null) {
    	    	towrite= read.split(",,");
    	        writer.print("<p> <span style=\"color:"+Colors[Integer.parseInt(towrite[0])]+";\">"+towrite[1]+", </span><span style=\"color:black;\">   "+ towrite[2]+" ,"+towrite[3]+" </span></p>");
    	    }
    	    
    	    writer.println("</body>");
    	    writer.println("</html>");
    	    s3is.close();
    	    reader.close();
    	    writer.close();
    	   // fos.close();
    	} catch (AmazonServiceException e) {
    	    System.err.println(e.getErrorMessage());
    	    System.exit(1);
    	} catch (FileNotFoundException e) {
    	    System.err.println(e.getMessage());
    	    System.exit(1);
    	} catch (IOException e) {
    	    System.err.println(e.getMessage());
    	    System.exit(1);
    	}
    }
    /* uplaodfile : 
     * upload the current input file to s3 under inputFile key with user bucket"
     */
    public static void uploadfile()
    {
    	System.out.println("uploading  : " + input_files[current_file].getName());
    	try {
    	s3.putObject(new PutObjectRequest(bucket_name, "inputFile", input_files[current_file]));
    	}
    	catch(Exception e)
    	{
    	   System.out.println("not here ");
    	   e.printStackTrace();	
    	}
    	System.out.println(" ..................done uploading file .............. ");
    }
    /*openq_send... : 
     * send task to manager  throw task_queue using the suitable message @param task_message*/
    public static void openq_sendmssg(String task_message)
    {
 
		SendMessageResult r = sqs.sendMessage(new SendMessageRequest(tasks_queue_url , task_message).withMessageGroupId(task_message).withDelaySeconds(0));
    }
   /*prepare_..:
    * build the task message using  local application variables and
    * @param t to determine should the manager shutdown in message
    * @return the message built */
    public static String prepare_msg(boolean t)
    {
    	return  new StringBuilder()
    		    .append(bucket_name)
    		    .append(",")
    		    .append(n)
    		    .append(",")
    		    .append(t)
    		    .append(",")
    		    .append(current_file)
    		    .toString();
    }
  /*checkMana.. :
   * checks for instance with tag name manger 
   * if no such exist start new instance with tag name "manager" 
   * if already exist return*/
    public static void checkManagerAndLaunch()  {
		System.out.print("*) Running Manager: ");
		List<Reservation> reserveList = ec2.describeInstances().getReservations();
		for (Reservation res : reserveList)
			for (Instance instance : res.getInstances())
				if (instance.getTags() != null)
					for (Tag tag : instance.getTags())
						if (tag.getKey().equals("Name") && tag.getValue().equals("Manager")
								&& instance.getState().getName().equals("running")) {
							System.out.println(" MANAGER IS ALREADY RUNNING.\n--------------------------------\n");
							return;
						}

		try {
			try {
				runInstance();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		System.out.println("DONE.\n--------------------------------\n");
	}
    /*runInstace:
     * init the manager instance with tags user script */
    @SuppressWarnings("deprecation")
	public static void runInstance()
			throws UnsupportedEncodingException, InterruptedException {
		//String str = new String(Base64.encodeBase64("wget https://manager15.s3-us-west-2.amazonaws.com/Manager-0.0.1-SNAPSHOT-jar-with-dependencies.jar".getBytes()), "UTF-8");
    	//String groupName = "VR23337";

    	
   	 
   	 /*********************************************
         *  
         *  #1.2 Describe Permissions.
         *  
         *********************************************/
   	 /*IpPermission ipPermission = new IpPermission();
   	 
   	// SSH Permissions
   	 ipPermission.withIpRanges("0.0.0.0/0")
    		            .withIpProtocol("tcp")
    		            .withFromPort(22)
    		            .withToPort(22);
    	
    	AuthorizeSecurityGroupIngressRequest authorizeSecurityGroupIngressRequest =	new AuthorizeSecurityGroupIngressRequest();
    	authorizeSecurityGroupIngressRequest.withGroupName(groupName).withIpPermissions(ipPermission);
    	ec2.authorizeSecurityGroupIngress(authorizeSecurityGroupIngressRequest);
        
        // HTTP Permissions
        ipPermission = new IpPermission();
        ipPermission.withIpRanges("0.0.0.0/0")
        				.withIpProtocol("tcp")
        				.withFromPort(80)
        				.withToPort(80);
        authorizeSecurityGroupIngressRequest =	new AuthorizeSecurityGroupIngressRequest();
        authorizeSecurityGroupIngressRequest.withGroupName(groupName).withIpPermissions(ipPermission);
        ec2.authorizeSecurityGroupIngress(authorizeSecurityGroupIngressRequest);
        CreateKeyPairRequest createKeyPairRequest = new CreateKeyPairRequest();
        String keyName = "VR23337.pem";
        createKeyPairRequest.withKeyName(keyName);           	
        CreateKeyPairResult createKeyPairResult = ec2.createKeyPair(createKeyPairRequest);
       	
        KeyPair keyPair = new KeyPair();	    	
        keyPair = createKeyPairResult.getKeyPair();           		    	
        try {
        String privateKey = keyPair.getKeyMaterial();
        File keyFile = new File(keyName);
        FileWriter fw;
		
			fw = new FileWriter(keyFile);
		
        fw.write(privateKey);
        fw.close();
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		System.out.println("\nCreate an Instance");
		RunInstancesRequest req =
				new RunInstancesRequest()
				.withImageId("ami-08d489468314a58df")
				.withInstanceType(InstanceType.T2Large)
				.withMinCount(1)
				.withMaxCount(1)
				.withKeyName("da")
				.withUserData(getScript())
				.withSecurityGroups("VR23337");
		RunInstancesResult result = ec2.runInstances(req);
		System.out.println("waiting");
		Thread.currentThread().sleep(18000);
		System.out.println("OK");
		List<Instance> resultInstance = result.getReservation().getInstances();
		String createdInstanceId = null;
		for (Instance ins : resultInstance){
			createdInstanceId = ins.getInstanceId();
			System.out.println("New instance has been created: "+ins.getInstanceId());
		}
		System.out.println("Create a 'tag' for the new instance.");
		List<String> resources = new LinkedList<String>();
		List<Tag> tags = new LinkedList<Tag>();
		Tag nameTag = new Tag("Name", "Manager");
		resources.add(createdInstanceId);
		tags.add(nameTag);
		CreateTagsRequest ctr = new CreateTagsRequest(resources, tags);
		ec2.createTags(ctr);
	}
/////////////////////////////////////prepare script for manager
	public static String getScript() throws UnsupportedEncodingException {
		ArrayList<String> script = new ArrayList<String>();
		script.add("#!/usr/bin/env bash");
		script.add("export " + "AWS_ACCESS_KEY_ID=\"AKIAIETFEW77FTVUWUNQ\"");
		script.add("export " + "AWS_SECRET_ACCESS_KEY=\"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\"");
		script.add("wget https://manager15.s3-us-west-2.amazonaws.com/Manager.jar");
		script.add("java -jar Manager.jar");
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

