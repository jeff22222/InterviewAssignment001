package test.test.test1;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;

/**
  * A starter example for writing Beam programs.
  *
  * <p>
  * The example takes two strings, converts them to their upper-case
  * representation and logs them.
  *
  * <p>
  * To run this starter example locally using DirectRunner, just execute it
  * without any additional parameters from your favorite development environment.
  *
  * <p>
  * To run this starter example using managed resource in Google Cloud Platform,
  * you should specify the following command-line options:
  * --project=<YOUR_PROJECT_ID>
  * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE> --runner=DataflowRunner
  */
public class EnterFile {
     private static final Logger LOG = LoggerFactory.getLogger(EnterFile.class);
     private static String HEADERS = "Row_ID,Order_ID,Order_Date,Ship_Date,Ship_Mode,Customer_ID,Customer_Name,Segment,City,State,Country,Postal_Code,Market,Region,Product_ID,Category,Sub_Category,Product_Name,Sales,Quantity,Discount,Profit,Shipping_Cost,Order_Priority";

    public static class FormatForBigquery extends DoFn<String, TableRow> {

        private String[] columnNames = HEADERS.split(",");

        @ProcessElement
         public void processElement(ProcessContext c) {
             TableRow row = new TableRow();
             String[] parts = c.element().split(",");

            if (!c.element().contains(HEADERS)) {
                 for (int i = 0; i < parts.length; i++) {
                     // No typy conversion at the moment.
                     row.set(columnNames[i], parts[i]);
                 }
                 c.output(row);
             }
         }

        /** Defines the BigQuery schema used for the output. */

        static TableSchema getSchemaOrder() {
             List<TableFieldSchema> fields = new ArrayList<>();
             // Currently store all values as String
            fields.add(new TableFieldSchema().setName("Row_ID").setType("DATE"));
            fields.add(new TableFieldSchema().setName("Order_ID").setType("DATE"));
            fields.add(new TableFieldSchema().setName("Order_Date").setType("DATE"));
        	fields.add(new TableFieldSchema().setName("Ship_Date").setType("DATE"));
        	fields.add(new TableFieldSchema().setName("Ship_Mode").setType("STRING"));
        	fields.add(new TableFieldSchema().setName("Customer_ID").setType("STRING"));
        	fields.add(new TableFieldSchema().setName("Customer_Name").setType("STRING"));
        	fields.add(new TableFieldSchema().setName("Segment").setType("STRING"));
        	fields.add(new TableFieldSchema().setName("City").setType("STRING"));
        	fields.add(new TableFieldSchema().setName("State").setType("STRING"));
        	fields.add(new TableFieldSchema().setName("Country").setType("STRING"));
        	fields.add(new TableFieldSchema().setName("Postal_Code").setType("STRING"));
        	fields.add(new TableFieldSchema().setName("Market").setType("STRING"));
        	fields.add(new TableFieldSchema().setName("Region").setType("STRING"));
        	fields.add(new TableFieldSchema().setName("Product_ID").setType("STRING"));
        	fields.add(new TableFieldSchema().setName("Category").setType("STRING"));
        	fields.add(new TableFieldSchema().setName("Sub_Category").setType("STRING"));
        	fields.add(new TableFieldSchema().setName("Product_Name").setType("STRING"));
        	fields.add(new TableFieldSchema().setName("Sales").setType("FLOAT"));       	
        	fields.add(new TableFieldSchema().setName("Quantity").setType("INTEGER"));
        	fields.add(new TableFieldSchema().setName("Discount").setType("FLOAT"));
        	fields.add(new TableFieldSchema().setName("Profit").setType("FLOAT"));
        	fields.add(new TableFieldSchema().setName("Shipping_Cost").setType("FLOAT"));
        	fields.add(new TableFieldSchema().setName("Order_Priority").setType("STRING"));



            return new TableSchema().setFields(fields);
         }
     }

    public static void main(String[] args) throws Throwable {
         // Currently hard-code the variables, this can be passed into as parameters
         String sourceFilePathOrder = "gs://fuckfile/Fuck/Orders.csv";
         boolean isStreaming = false;

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
         // This is required for BigQuery
         options.setJobName("CSVtoBQ");
         Pipeline p = Pipeline.create(options);

        p.apply("Read CSV File", TextIO.read().from(sourceFilePathOrder))
                 .apply("Log messages", ParDo.of(new DoFn<String, String>() {
                     @ProcessElement
                     public void processElement(ProcessContext c) {
                         LOG.info("Processing row: " + c.element());
                         c.output(c.element());
                     }
                 })).apply("Convert to BigQuery TableRow", ParDo.of(new FormatForBigquery()))
                 .apply("Write into BigQuery",
                         BigQueryIO.writeTableRows().to("fuck-260904:Test.Orders_FuckTest").withSchema(FormatForBigquery.getSchemaOrder())
                                 .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                 .withWriteDisposition(isStreaming ? BigQueryIO.Write.WriteDisposition.WRITE_APPEND
                                         : BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        p.run().waitUntilFinish();
        

    }
}