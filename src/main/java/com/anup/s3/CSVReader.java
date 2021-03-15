package com.anup.s3;

import com.anup.model.Customer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import static com.anup.s3.FileConstants.FILE_PATH;
import static com.anup.s3.FileConstants.HEADERS;


public class CSVReader {
    public static void main(String[] args) throws IOException {
        readCSVFile();
    }

    public static void readCSVFile() throws IOException {
        Reader in = new FileReader(new File(FILE_PATH).getAbsolutePath());
        Iterable<CSVRecord> records = CSVFormat.DEFAULT
                .withHeader(HEADERS)
                .withFirstRecordAsHeader()
                .parse(in);
        for (CSVRecord record : records) {
            String id = record.get("id");
            String name = record.get("name");
            String email = record.get("email");
            Customer customer = new Customer(id, name, email);
            System.out.println("Record from file --> " + customer);
        }
    }
}
