import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import com.google.gson.*;


import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class User {
	public  String bucket_id;
	public  List<Review> reviews = null;
	public AmazonS3 s3;
	public List<String> results ; 
	public User()
	{
		
	}
	/*constructor 
	 * manager user init the user with bucket id 
	 * */
	public User(String buck_id)
	{
		this.bucket_id=buck_id;
		results= new ArrayList<String>();
		
	}
	/*parse_reviews.:
	 * use @param s3 variable given from task_manager to connect s3
	 * connect to bucket with bucket_id 
	 * get input file 
	 * read it and parse it into reviews 
	 * add all reviews into @param reviews list
	 * */
	public  void parse_reviews(AmazonS3 s3)
	{
		InputStream in;
		S3Object o = s3.getObject(new GetObjectRequest(this.bucket_id, "inputFile"));
	    in = o.getObjectContent();
		reviews = new ArrayList<Review>();
		try {
			//in = new FileInputStream("in1.txt");
			Gson g = new GsonBuilder()
			        .setLenient()
			        .create();
			BufferedReader r = new BufferedReader(new InputStreamReader(in));
			String line;
			Title titles;
			while ((line= r.readLine())!=null) {
		 titles = g.fromJson(line, Title.class);
		     reviews.addAll(titles.getReviews());
			  }
			r.close();
			}
		
		 catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public String get_bucket_id()
	{
		return this.bucket_id;
	}
	public List<Review> get_reviews()
	{
		return this.reviews;
	}
}

