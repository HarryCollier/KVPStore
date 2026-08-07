import java.util.ArrayList;
import java.util.List;

public class Shard {
    private int leader;
    //uniquely identifying name
    private int id;
    private List<Integer> followers;

    //default constructor for jackson
    public Shard() {
        followers = new ArrayList<>();
    }

    //first node added is going to be the leader, so just initiate followers
    public Shard(int id, int leader) {
        this.leader = leader;
        this.id = id;
        this.followers = new ArrayList<>();
    }
    //getters
    public int getLeader() {return leader;}
    public int getId() {return id;}
    public List<Integer> getFollowers() {return followers;}

    //adds follower
    public void addFollower(int follower) {followers.add(follower);}
}
