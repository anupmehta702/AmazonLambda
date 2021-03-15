package com.anup.s3;

import com.anup.model.Customer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.anup.s3.FileConstants.FILE_PATH;
import static com.anup.s3.FileConstants.HEADERS;


public class CSVWriter {


    public static void main(String[] args) throws IOException {
        writeToCSVFile(new Customer("111","aaa","aaa@email.com"));
    }

    public static void writeToCSVFile(Customer customer) throws IOException {
        System.out.println("Writing to CSV file ..");
        List<Customer> customerList = new ArrayList<>();
        customerList.add(customer);
        FileWriter out = new FileWriter(new File(FILE_PATH).getAbsolutePath());
        try(final CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.withHeader(HEADERS))){
            customerList.forEach(record -> {
                try {
                    printer.printRecord(record.getId(),record.getName(),record.getEmail());
                } catch (IOException e) {
                    System.out.println("Error while writing record -->"+e.getMessage());
                    e.printStackTrace();
                }
            } );
        } catch (IOException e) {
            System.out.println("Error while creating CSVPrinter --> "+e.getMessage());
            e.printStackTrace();
        }

    }



}
