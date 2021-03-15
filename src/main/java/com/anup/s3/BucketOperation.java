package com.anup.s3;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import static com.anup.s3.FileConstants.*;

public class BucketOperation {

    public static String BUCKET_NAME = "mycustomerbucket702";
    private static final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).build();

    public static void main(String[] args) throws FileNotFoundException {
        System.out.println("Bucket Operation ....");
        //  createBucket();
        //Bucket b = getBucket();
        //System.out.println("bucket with name ->"+b.getName()+" retrieved successfully !");
        //   putObject();
        listObject();
        downloadObject();
    }

    private static void downloadObject() {
        System.out.println("Downloading object from S3 bucket - "+BUCKET_NAME);
        try {
            S3Object s3Object = s3.getObject(BUCKET_NAME, FILE_NAME);
            S3ObjectInputStream s3is = s3Object.getObjectContent();
            FileOutputStream fileToWrite = new FileOutputStream(new File(DOWNLOAD_FILE_PATH));
            byte[] read_buf = new byte[1024];
            int read_len = 0;
            while ((read_len = s3is.read(read_buf)) > 0) {
                fileToWrite.write(read_buf, 0, read_len);
            }
            s3is.close();
            fileToWrite.close();
        } catch (IOException e) {
            System.out.println("Exception while downloading the file --> "+e.getMessage());
            e.printStackTrace();
        }
        System.out.println("File downloaded from S3 with name -->"+DOWNLOAD_FILE_PATH);
    }

    private static void listObject() {
        System.out.println("Listing objects ...");
        ListObjectsV2Result result = s3.listObjectsV2(BUCKET_NAME);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        for (S3ObjectSummary os : objects) {
            System.out.println("Object key --> " + os.getKey() + " Bucket Name --> " + os.getBucketName());
        }
    }

    private static void createBucket() {
        System.out.println("Create Bucket operation ...");
        /*try {
            Bucket newBucket = s3.createBucket(BUCKET_NAME);
        }catch(AmazonS3Exception e){
            System.out.println("Exception while creating S3 bucket --> "+BUCKET_NAME);
            e.printStackTrace();
        }*/
        if (s3.doesBucketExistV2(BUCKET_NAME)) {
            System.out.println("Bucket with name --> " + BUCKET_NAME + " already exists !");
        } else {
            try {
                Bucket b = s3.createBucket(BUCKET_NAME);
            } catch (AmazonS3Exception e) {
                System.out.println("Exception while creating S3 bucket --> " + BUCKET_NAME);
                e.printStackTrace();
            }

        }
        System.out.println("Bucket --> " + BUCKET_NAME + " created successfully !");
    }

    private static Bucket getBucket() {
        System.out.println("Get bucket operation ...");
        List<Bucket> existingBuckets = s3.listBuckets();
        for (Bucket bucket : existingBuckets) {
            if (bucket.getName().equalsIgnoreCase(BUCKET_NAME)) {
                return bucket;
            }
        }
        return null;
    }

    public static void putObject() {
        try {
            File fileToUpload = new File(FILE_PATH);
            String key_name = fileToUpload.getName();
            s3.putObject(BUCKET_NAME, FILE_NAME, fileToUpload);
        } catch (AmazonS3Exception e) {
            e.printStackTrace();
        }
    }

}
