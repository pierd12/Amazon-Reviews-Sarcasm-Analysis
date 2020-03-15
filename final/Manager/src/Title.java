
import java.util.List;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;


public class Title {

@SerializedName("title")
@Expose
private String title;
@SerializedName("reviews")
@Expose
private List<Review> reviews = null;

/**
* No args constructor for use in serialization
*
*/
public Title () {
}

/**
*
* @param reviews
* @param title
*/
public Title (String title, List<Review> reviews) {
super();
this.title = title;
this.reviews = reviews;
}

public String getTitle() {
return title;
}

public void setTitle(String title) {
this.title = title;
}

public List<Review> getReviews() {
return reviews;
}

public void setReviews(List<Review> reviews) {
this.reviews = reviews;
}

@Override
public String toString() {
return "";//new ToStringBuilder(this).append("title", title).append("reviews", reviews).toString();
}

}
