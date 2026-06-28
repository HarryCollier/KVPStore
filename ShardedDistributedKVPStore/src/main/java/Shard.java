import java.util.ArrayList;
import java.util.List;

public class Shard {
    private int leader;
    //uniquely identifying name
    private int id;
    private List<Integer> followers;

    //first node added is going to be the leader, so just initiate followers
    public Shard(int leader, int id) {
        this.leader = leader;
        this.id = id;
        this.followers = new ArrayList<>();
    }
    //getters
    public int getLeader() {return leader;}
    public String getId() {return id;}
    public List<Integer> getFollowers() {return followers;}

    //adds follower
    public void addFollower(int follower) {followers.add(follower);}
}
