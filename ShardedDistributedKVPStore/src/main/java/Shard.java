import java.util.ArrayList;
import java.util.List;

public class Shard {
    private Address leader;
    private int id;
    private List<Address> followers;

    public Shard() {
        followers = new ArrayList<>();
    }

    public Shard(int id, Address leader) {
        this.leader = leader;
        this.id = id;
        this.followers = new ArrayList<>();
    }

    public Address getLeader() { return leader; }
    public int getId() { return id; }
    public List<Address> getFollowers() { return followers; }

    public void setLeader(Address leader) { this.leader = leader; }
    public void setId(int id) { this.id = id; }
    public void setFollowers(List<Address> followers) { this.followers = followers; }

    public void addFollower(Address follower) { followers.add(follower); }
}