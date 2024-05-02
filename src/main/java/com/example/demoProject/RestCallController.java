package com.example.demoProject;


import java.util.List;
import java.util.Map;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.Type;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;

//@RestController
public class RestCallController {


    public static void main(String[] args) {
        AWSCredentials credentials = new BasicAWSCredentials("<accesskey>", "<secretkey>");
        AmazonS3 s3client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials)).withRegion("<region>").build();

        Configuration conf = new Configuration();
        conf.set("fs.s3a.access.key", "");
        conf.set("fs.s3a.secret.key", "");

        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        String fullPath = "s3a://" + "<bucketName>" + "/" + "test_files/part-00000-75d1dcfe-d9f8-4ad4-9633-966be7739c4e.c000.snappy.parquet";
        try {

            ParquetMetadata metadata = ParquetFileReader.readFooter(conf, new Path(fullPath), ParquetMetadataConverter.NO_FILTER);
            List<Type> columnsDescriptorsFromSource = metadata.getFileMetaData().getSchema().getFields();
            Map<String, String> keyValueMetaData = metadata.getFileMetaData().getKeyValueMetaData();
            System.out.println(" keyValueMetaData: " + keyValueMetaData.toString());
            System.out.println(columnsDescriptorsFromSource.toString());

            System.out.println(" metadata ");
        } catch (Exception e) {

            System.out.println(" " + e.getMessage());
        }

    }


}





