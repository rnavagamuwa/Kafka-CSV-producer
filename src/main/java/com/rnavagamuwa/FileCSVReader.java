package com.rnavagamuwa;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * @author rnavagamuwa
 */
public class FileCSVReader implements CSVReader {
    @Override
    public BufferedReader readCSV() {
        try {
            return new BufferedReader(new FileReader(CLI.csvLocation));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File cannot be found : " + CLI.csvLocation);
        }
    }
}
