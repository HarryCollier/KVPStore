public class Command {
    private String type;
    private String key;
    private String value;

    /**
     * basic constructor for reflection when converting to json
     */
    public Command() {}

    /**
     * 
     * @param type the type of the command
     * @param key the key of the KVP
     * @param value the value of the KVP
     */
    public Command (String type, String key, String value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    /**
     * 
     * @param request the request that the command parses to fill its attributes
     * 
     * parses a command to fill the type, key and value attribtues, throwing an error if the request is invalid
     */
    public Command(String request){
        //if request is null or purely whitespace throw an error
        if (request == null || request.trim().isEmpty()) {
            throw new IllegalArgumentException("Null/Empty request recieved");
        }

        // seperate the data into [TYPE, KEY, VALUE] type/key is the 1st/2nd word in the request, the rest is stored in value
        String[] data = request.split(" ", 3);
        //if the length of this array is less than 2 then it is invalid and throw an error
        if (data.length < 2) {
            throw new IllegalArgumentException("Server only accepts inputs in the form: GET/DELETE KEY or PUT KEY VALUE");
        }
        //fill in type key and key
        this.type = data[0];
        this.key = data[1];

        //set value to empty if there isnt a value provided in the request, otherwise set it to the value provided
        this.value = data.length < 3 ? "" : data[2];
    }

    //GETTERS for type key and value
    public String getType() {return this.type;}
    public String getKey() {return this.key;}
    public String getValue() {return this.value;}
}