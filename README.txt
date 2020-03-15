#mhmd mhameed 314993130
#pier damouny 316397256 
#Running the program : 
    1) download the source files compile them with 
        1.1)mvn clean 
        1.2)mvn install 
    2) upload the jar files of Worker and Manager to s3 under Mnager15 bucket wich we used to downlaod jar in our code 
    3) run the Local application with input files fllow by output files and n  (raito of instaces to reviews) and "terminate" if needed.
    4) the local appliction will tell when every output file is ready .
 
  
#Running time Approx 15 min using T2.larg instaces with suggested Image and 8 instanes .

#######################################################################
Program Flow : 
    1) Local app : 
        1.1) start a bucket name with user name made bucket in s3 with that name .
        1.2) make queue with name bucketname.fifo for results of workers
        1.3) make queue with name bucketname.out for results from manger to local.
        1.4)loop start : 
        1.5) upload input file 1 to bucket and send messgae to manager .
        1.6) wait for result from manger on queue
        1.7) download output and make hmtl file 
        1.8) pick next input file and repeat 1.4
        1.9) close every queue and bucket and terminate .
    2) Manager : 
        2.1) make queue for task from loacl and queue for review to send to worker
             -s and other data members.
        2.2) start loop 
        2.3) recieve message from local and analyze it .
        2.4) allcate thread from threadpool to take care of local app and back to               2.2 .
        2.5) thread downlaod file from s3 and parse reviews from files 
        2.6) thread push review to review queue for workers to read from.
        2.7) thread start workers if needed and wait for results in queueresult.
        2.8) thread upload result to s3 and send message for local user.
        2.9) thread termiante and back to recieve more jobs .
        2.10) manager main thread after recive terminate message will wait for 
             stop recieveing messages and wair for all thread to finish then                    terminates.
    3) worker : 
      3.1) start data members to use for analyzing reviews .
      3.2)start loop works to die 
      3.3) read messages from reviews queue 
      3.4) analyze message and get result .
      3.5) send result to thier respective queues .
      3.5) repeat 3.2
      
##########Security :  we thought about Security and we decided to pass credentials using user data and hard coded and uploaded jar to s3 .



#############3#Scalability :  scalability is our most important therefore the manager multithreaded that can scalable as many threades as possible and add to that each user will get one thread at time and one file in manager therefore will give more users to get served and distrupte computing power on as many user as we can , the worker are independent computing units that recieve messaes and send result and therfore increasing number messages will add work to the workers and by increasing number of workers we can make the system run faster therefore its scalable .


############Fail Cases : if a worker instace fail its only meant the will no longerrecive message without affecting other elemnts in the system and thanks to visisbilty timeout if worker cant send the result and delete the message he recieved from the queue and message will be available for other workers to handle therefore no messages will be lost during any fail .



#################System Follow is tested with multiple local users and different inputs and its working with most efficiency and thread are only added to manager to handle mutiple users at time and no other application require threads . workers will work in parrel in most of times as much they is review to analyze and try to get and analyze review at once as they without waitng for queues for long time and only when no enough reviews available some wroker will start to wait for messages .




###################termiantion : upon recieving termination message the manager will stop the main loop and therefore will stop recieving messages and it will wait for threads / local users to be finished ( that already started ) and then terminate all queues and close the manager application and terminate all workers instaces  .



#################Correctness of the system : the system is fully distrupted as if every part of its doing is job separatly without interrupting others job  as the local only uplaod down load input and output files and write html files , manager recieve review parse them and send them to queue and receive messages result from workers and uplaod them to s3 , workers receive reviews analyze them and return results . all the parts coming together using the power of AWS using sqs,s3 as pipeline to communicate 
and the follow is efficient.






- Note there maybe some problems in matter of credentials and lunching workers because the system biult to use our credentials and securtyGroups if you want to run the system with yours and you failed ,you can contact us on mhmd@post.bgu.ac.il and we will help you run it . ( there is no problems but in case  of any , the systm works perfectly ). thanks !! 