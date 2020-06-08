package org.wumiguo.io;

import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Created by levinliu on 2020/6/5
 * GitHub: https://github.com/levinliu
 * (Change file header on Settings -> Editor -> File and Code Templates)
 */
public class Data2Csv {
    public static void data2csv(String csvPath, String[] headers, List<String[]> data) throws IOException {
        File file = new File(csvPath);
        try (FileWriter outputFile = new FileWriter(file)) {
            try (CSVWriter writer = new CSVWriter(outputFile)) {
                writer.writeNext(headers);
                writer.writeAll(data);
            }
        }
    }
}

