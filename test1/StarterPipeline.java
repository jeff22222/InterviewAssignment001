package test.test.test1;
import java.lang.String;
import org.apache.beam.sdk.extensions.jackson.AsJsons;
import com.google.gson.JsonParser;
import java.util.HashMap;
import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.lang.Object;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;


import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;


public class StarterPipeline {
	  


            public static class splitOrderID extends DoFn<String, String>{
			@ProcessElement
		    	public void processElement(ProcessContext data){
				Gson gson = new Gson();
				JsonObject row = new JsonParser().parse(data.element()).getAsJsonObject();
					String old_product[] = row.get("Order_ID").getAsString().split("-");
					row.addProperty("OrderType", old_product[0]);
					row.addProperty("OrderYear", old_product[1]);
					row.addProperty("OrderNumber", old_product[2]);
					row.remove("Order_ID");
				data.output(gson.toJson(row));
			}
		    }

		    public static class splitProductID extends DoFn<String, String>{
			@ProcessElement
		    	public void processElement(ProcessContext data){
				Gson gson = new Gson();
				JsonObject row = new JsonParser().parse(data.element()).getAsJsonObject();
					String old_order[]  = row.get("Product_ID").getAsString().split("-");
					row.addProperty("CategoryID", old_order[0]);
					row.addProperty("SubCategoryID", old_order[1]);
					row.addProperty("ProductNumber", old_order[2]);
					row.remove("Product_ID");
				data.output(gson.toJson(row));
			}
		    }
 
		    public static class splitModePriority extends DoFn<String, String>{
			@ProcessElement
		    	public void processElement(ProcessContext data){
				Gson gson = new Gson();
				JsonObject row = new JsonParser().parse(data.element()).getAsJsonObject();
					String old_order[]  = row.get("ModePriority").getAsString().split(",");
					row.addProperty("Sub_Priority", old_order[0]);
					row.addProperty("Sub_Mode", old_order[1]);
					row.remove("ModePriority");
				data.output(gson.toJson(row));
			}
		    }
		    
		    public static class calculateCost extends DoFn<String, String>{
		    	@ProcessElement
			public void processElement(ProcessContext data){
				Gson gson = new Gson();
				JsonObject row = new JsonParser().parse(data.element()).getAsJsonObject();
				float sales = row.get("Sales").getAsFloat();
				float profit = row.get("Profit").getAsFloat();
				row.addProperty("Cost", sales+profit);
				row.addProperty("Margin", profit/sales);
				data.output(gson.toJson(row));
			}
		    }


			
		    public static void main(String[] args) {
		    	String TargetTable = "SELECT Order1.Order_ID, Order_Date, Ship_Date, Ship_Mode, Customer_ID, Customer_Name, Segment, City, State, Country, Postal_Code, Market, Region, Product_ID, Category, Sub_Category, Product_Name, Sales, Quantity, Discount, Profit, Shipping_Cost, Order_Priority,string_field_0 as People,STRING_AGG(ModePriority) as ModePriority FROM Test.Orders_Fuck as Order1 INNER JOIN Test.People_Fuck as People1 ON Region=string_field_1 INNER JOIN Test.Mode_Fuck as Mode1 On Order1.Order_ID=Mode1.Order_ID GROUP BY Order1.Order_ID,Order_Date, Ship_Date, Ship_Mode, Customer_ID, Customer_Name, Segment, City, State, Country, Postal_Code, Market, Region, Product_ID, Category, Sub_Category, Product_Name, Sales, Quantity, Discount, Profit, Shipping_Cost, Order_Priority,string_field_0;";
		         String tempLocationPath = "gs://fuckfile/temp";
		         boolean isStreaming = false;
		    	PipelineOptions pop = PipelineOptionsFactory.fromArgs(args).withValidation().create();
		    	pop.setTempLocation(tempLocationPath);
		        pop.setJobName("csvtobq");
		    	Pipeline pipeline = Pipeline.create(pop);
		    	
		    	Gson gson = new Gson();
		        
		    	PCollection<String> CombinedFiles = pipeline		
		    	.apply("Read from BQ", BigQueryIO.readTableRows().fromQuery(TargetTable).usingStandardSql())
				.apply("Tablerow to Json", AsJsons.of(TableRow.class));
			CombinedFiles
				.apply("Split ProductID", ParDo.of(new splitProductID()))
				.apply("Split OrderID", ParDo.of(new splitOrderID()))
				.apply("Split ModePriority", ParDo.of(new splitModePriority()))
				.apply("Calculate Margin and Cost", ParDo.of(new calculateCost()))
				.apply("Writing Result to GCS", TextIO.write().to("gs://fuckfile/Fuck/Result-0004"));

           
		        pipeline.run();
		    }

}