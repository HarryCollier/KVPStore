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

    public String prettyPrint() {
        StringBuilder sb = new StringBuilder();
        sb.append("Shard{id=").append(id).append(", leader=").append(leader.prettyPrint()).append(", followers=[");
        for (Address follower : followers) {
            sb.append(follower.prettyPrint()).append(", ");
        }
        if (!followers.isEmpty()) {
            sb.setLength(sb.length() - 2); // Remove last comma and space
        }
        sb.append("]}");
        return sb.toString();
    }
}