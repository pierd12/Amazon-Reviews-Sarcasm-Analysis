

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;


public class Review {

@SerializedName("id")
@Expose
private String id;
@SerializedName("link")
@Expose
private String link;
@SerializedName("title")
@Expose
private String title;
@SerializedName("text")
@Expose
private String text;
@SerializedName("rating")
@Expose
private int rating;
@SerializedName("author")
@Expose
private String author;
@SerializedName("date")
@Expose
private String date;

/**
* No args constructor for use in serialization
*
*/
public Review() {
}

/**
*
* @param date
* @param author
* @param link
* @param rating
* @param id
* @param text
* @param title
*/
public Review(String id, String link, String title, String text, int rating, String author, String date) {
super();
this.id = id;
this.link = link;
this.title = title;
this.text = text;
this.rating = rating;
this.author = author;
this.date = date;
}

public String getId() {
return id;
}

public void setId(String id) {
this.id = id;
}

public String getLink() {
return link;
}

public void setLink(String link) {
this.link = link;
}

public String getTitle() {
return title;
}

public void setTitle(String title) {
this.title = title;
}

public String getText() {
return text;
}

public void setText(String text) {
this.text = text;
}

public int getRating() {
return rating;
}

public void setRating(int rating) {
this.rating = rating;
}

public String getAuthor() {
return author;
}

public void setAuthor(String author) {
this.author = author;
}

public String getDate() {
return date;
}

public void setDate(String date) {
this.date = date;
}

@Override
public String toString() {
	Gson g = new GsonBuilder()
	        .setLenient()
	        .create();
	String ans =
	g.toJson(this);
	//System.out.println(ans);
return ans;
}

}
