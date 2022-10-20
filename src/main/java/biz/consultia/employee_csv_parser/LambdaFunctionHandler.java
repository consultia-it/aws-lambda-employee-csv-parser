package biz.consultia.employee_csv_parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

	/** The DynamoDB table name. */
	String DYNAMO_TABLE_NAME = "employee";

	private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
	private AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.EU_WEST_3).build();

	@Override
    public String handleRequest(S3Event event, Context context) {
        context.getLogger().log("Received event: " + event);

        // Get the object from the event and show its content type
        String bucket = event.getRecords().get(0).getS3().getBucket().getName();
        String key = event.getRecords().get(0).getS3().getObject().getKey();
        
        S3Object response = null;
        String contentType;
        try {
            response = s3.getObject(new GetObjectRequest(bucket, key));
            contentType = response.getObjectMetadata().getContentType();
            context.getLogger().log("CONTENT TYPE: " + contentType);            
        } 
        catch (Exception e) {
            e.printStackTrace();
            context.getLogger().log(String.format(
                "Error getting object %s from bucket %s. Make sure they exist and your bucket is in the same region as this function.", key, bucket));
            throw e;
        }
        
        parseCSVS3Object(response);
        
		return contentType;
    }

	private void parseCSVS3Object(S3Object data) {
		BufferedReader reader = new BufferedReader(new InputStreamReader(data.getObjectContent()));
        
		try {
			String line;
			while ((line = reader.readLine()) != null) {
				String[] row = line.split(",");
				putItemInTable(dynamoDBClient, row[0], row[1], row[2]);
			}
		} 
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void putItemInTable(AmazonDynamoDB ddb, String employeeId, String employeeName, String employeeSalary) {

		HashMap<String, AttributeValue> itemValues = new HashMap<>();
		itemValues.put("id", new AttributeValue().withN(employeeId));
		itemValues.put("name", new AttributeValue(employeeName));
		itemValues.put("salary", new AttributeValue().withN(employeeSalary));

		PutItemRequest request = new PutItemRequest(DYNAMO_TABLE_NAME, itemValues);

		try {
			ddb.putItem(request);
			System.out.println(DYNAMO_TABLE_NAME + " was successfully updated");
		} 
		catch (ResourceNotFoundException e) {
			System.err.format("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", DYNAMO_TABLE_NAME);
			System.err.println("Be sure that it exists and that you've typed its name correctly!");
			System.exit(1);
		} 
		catch (AmazonDynamoDBException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
	}
}