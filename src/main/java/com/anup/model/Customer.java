package com.anup.model;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Customer {
    private final static ObjectMapper JSON = new ObjectMapper();
    static {
        JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
    private String id;
    private String name;
    private String email;

        public byte[] toJsonAsBytes() {
        try {
            return JSON.writeValueAsBytes(this);
        } catch (IOException e) {
            return null;
        }
    }

    public static Customer fromJsonAsBytes(byte[] bytes) {
        try {
            return JSON.readValue(bytes, Customer.class);
        } catch (IOException e) {
            return null;
        }
    }


    public static Customer setData(StreamRecord record){
        Customer customer = new Customer();
        Map<String, AttributeValue> updatedMap = record.getNewImage() != null ? record.getNewImage() : record.getOldImage();

        updatedMap.forEach((key,value)-> {
            try {
                String variable = key.substring(0, 1).toUpperCase() + key.substring(1);
                Method setterMethod = Customer.class.getDeclaredMethod("set"+variable,String.class);
                setterMethod.invoke(customer,value.getS());
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
        });
        return customer;
    }


    public static List<Customer> getCustomers() {
        List<Customer> customerList = new ArrayList<>();
        customerList.add(new Customer("1","Anup","anupmehta702@gmail.com"));
        customerList.add(new Customer("2","abc","abc@gmail.com"));
        customerList.add(new Customer("3","def","def@gmail.com"));
        return customerList;
    }
}
