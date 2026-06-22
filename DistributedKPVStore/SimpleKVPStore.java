import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

public class SimpleKVPStore {
    private Properties store;
    private String fileName;

    /**
     * @param fileName File name of the properties file to be used
     * 
     * creates store, sets file name, and loads any existing data into the store (loads nothing if data not found)
     */
    public SimpleKVPStore(String fileName) {
        this.store = new Properties();
        this.fileName = fileName;

        loadData();
    }

    /**
     * loads data into the store, loads nothing if no file with fileName is found
     */
    public void loadData() {
        try (FileInputStream in = new FileInputStream(this.fileName)) {
            store.load(in);
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
            store.store(out, "Simple KVP store");
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
     * locked to prevent one thread overwriting another
     */
    public synchronized Boolean put(String key, String value) {
        //set property then instantly save
        store.setProperty(key, value);
        saveData();
        return true;
    }

    /**
     * @param key The key to get
     * 
     * @return the value associated with this key
     * gets the value stored under the key, returns null if not present
     * locked to prevent one thread overwriting another
     */
    public synchronized String get(String key) {
        return store.getProperty(key);
    }

    /**
     * @param key the key to delete
     * 
     * @return whether the delete was sucessfull
     * 
     * takes a key and deletes that KVP from the store
     * locked to prevent one thread overwriting another
     */
    public synchronized boolean remove(String key) {
        //remove and return value removed
        Object value = store.remove(key);
        saveData();
        // if value is null then it was never in the store, so return false
        return value != null;
    }
}