import java.io.*;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

// Abstract class for Serializer
abstract class Serializer {
    public abstract String serialize(Map<String, Object> data);

    public abstract Map<String, Object> deserialize(String data);
}

// JSONSerializer implementation
class JSONSerializer extends Serializer {
    @Override
    public String serialize(Map<String, Object> data) {
        StringBuilder json = new StringBuilder("{");
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            json.append("\"").append(entry.getKey()).append("\":");
            if (entry.getValue() instanceof String) {
                json.append("\"").append(entry.getValue()).append("\"");
            } else {
                json.append(entry.getValue());
            }
            json.append(",");
        }
        json.deleteCharAt(json.length() - 1);
        json.append("}");
        return json.toString();
    }

    @Override
    public Map<String, Object> deserialize(String data) {
        Map<String, Object> result = new HashMap<>();
        data = data.substring(1, data.length() - 1);
        String[] pairs = data.split(",");
        for (String pair : pairs) {
            String[] keyValue = pair.split(":");
            String key = keyValue[0].replaceAll("\"", "").trim();
            String value = keyValue[1].replaceAll("\"", "").trim();
            result.put(key, value);
        }
        return result;
    }
}

// Storage class
class Storage {
    private String logFilename;
    private Serializer serializer;

    public Storage(String logFilename, Serializer serializer) {
        this.logFilename = logFilename;
        this.serializer = serializer;
        initLog();
    }

    private void initLog() {
        if (!Files.exists(Paths.get(logFilename))) {
            try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(logFilename))) {
                System.out.println("Initialized log file in write mode: " + logFilename);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void appendLog(String operation, String key, String value) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        Map<String, Object> logEntry = new HashMap<>();
        logEntry.put("timestamp", timestamp);
        logEntry.put("operation", operation);
        logEntry.put("key", key);
        logEntry.put("value", value);

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(logFilename), StandardOpenOption.APPEND)) {
            writer.write(serializer.serialize(logEntry) + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<Map<String, Object>> readLog() {
        List<Map<String, Object>> res = new ArrayList<>();
        if (Files.exists(Paths.get(logFilename))) {
            try (BufferedReader reader = Files.newBufferedReader(Paths.get(logFilename))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    res.add(serializer.deserialize(line.trim()));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return res;
    }
}

// Database class
class Database {
    private Storage storage;
    Map<String, String> db;

    public Database(Storage storage) {
        this.storage = storage;
        this.db = new HashMap<>();
        recoverFromLog();
    }

    private void recoverFromLog() {
        List<Map<String, Object>> logEntries = storage.readLog();
        for (Map<String, Object> entry : logEntries) {
            String key = (String) entry.get("key");
            String value = (String) entry.get("value");
            String operation = (String) entry.get("operation");
            if ("INSERT".equals(operation) || "UPDATE".equals(operation)) {
                db.put(key, value);
            }
        }
    }

    public void insert(String key, String value) {
        if (db.containsKey(key)) {
            throw new IllegalArgumentException("Key " + key + " already exists");
        }
        db.put(key, value);
        storage.appendLog("INSERT", key, value);
    }

    public void update(String key, String value) {
        if (!db.containsKey(key)) {
            throw new IllegalArgumentException("Key " + key + " does not exist");
        }
        db.put(key, value);
        storage.appendLog("UPDATE", key, value);
    }

    public String select(String key) {
        return db.getOrDefault(key, null);
    }
}

// Main class
public class Main {
    public static void main(String[] args) {
        Serializer serializer = new JSONSerializer();
        Storage storage = new Storage("db.log", serializer);
        Database database = new Database(storage);
        System.out.println("@@ db: " + database.db);
    }
}