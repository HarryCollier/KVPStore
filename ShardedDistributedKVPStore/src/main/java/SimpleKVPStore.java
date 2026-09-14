import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleKVPStore {
    private final Map<String, String> store;
    private final String fileName;

    /**
     * @param fileName File name of the properties file to be used
     * 
     * creates store, sets file name, and loads any existing data into the store (loads nothing if data not found)
     */
    public SimpleKVPStore(String fileName) {
        this.store = new ConcurrentHashMap<>();
        this.fileName = fileName;

        loadData();
    }

    /**
     * loads data into the store, loads nothing if no file with fileName is found
     */
    public void loadData() {
        try (FileInputStream in = new FileInputStream(this.fileName)) {
            Properties props = new Properties();
            props.load(in);
            for (String key : props.stringPropertyNames()) {
                store.put(key, props.getProperty(key));
            }
        }
        catch (IOException e) {
            System.out.println("No existing data found");
        }
    }

    /**
     * saves the data currently in the store to the correct file
     */
    public void saveData() {
        try (FileOutputStream out = new FileOutputStream(this.fileName)) {
            Properties props = new Properties();
            props.putAll(store);
            props.store(out, "Simple KVP store");
        }
        catch (IOException e) {
            System.out.println("Failed to save data: " + e.getMessage());
        }
    }

    /**
     * @param key The key to store under
     * @param value The value to store
     * 
     * @return whether the key was added
     * adds the mapping key --> value to the store, returning True if added, False otherwise
     */
    public Boolean put(String key, String value) {
        store.put(key, value);
        return true;
    }

    /**
     * @param key The key to get
     * 
     * @return the value associated with this key
     * gets the value stored under the key, returns null if not present
     */
    public String get(String key) {
        return store.get(key);
    }

    /**
     * @param key the key to delete
     * 
     * @return whether the delete was sucessfull
     * 
     * takes a key and deletes that KVP from the store
     */
    public boolean remove(String key) {
        return store.remove(key) != null;
    }
}