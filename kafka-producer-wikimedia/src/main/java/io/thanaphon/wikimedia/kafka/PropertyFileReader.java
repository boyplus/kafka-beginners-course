package io.thanaphon.wikimedia.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PropertyFileReader {
    public String getPropValues(String key) throws IOException {
        try {
            Properties prop = new Properties();

            File src = new File("config.properties");
            FileInputStream fis = new FileInputStream(src);
            prop.load(fis);

            return prop.getProperty(key);

        } catch (Exception e) {
            System.out.println("Exception: " + e);
        }
        return "";
    }
}