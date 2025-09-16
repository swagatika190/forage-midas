package com.jpmc.midascore;

import org.springframework.stereotype.Component;
import java.nio.charset.StandardCharsets;
//import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

import java.io.InputStream;

@Component
public class FileLoader {
    public String[] loadStrings(String path) {
        try {
            InputStream inputStream = this.getClass().getResourceAsStream(path);
            if (inputStream == null) {
                throw new RuntimeException("File not found in resources: " + path);
            }
            String fileText = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            return fileText.split(System.lineSeparator());
        } catch (Exception e) {
        	throw new RuntimeException("Failed to read file: " + path, e);  
        }
    }
}

