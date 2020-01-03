package test.test.test1;
import java.lang.String;
import org.apache.beam.sdk.extensions.jackson.AsJsons;
import com.google.gson.JsonParser;
import java.util.HashMap;
import java.lang.reflect.Type;
import com.google.gson.reflect.TypeToken;

import test.test.test1.StarterPipeline.splitModePriority;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.lang.Object;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
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
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;






class Test {

    public static class splitProductID extends DoFn<String, String>{
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

    public static class splitOrderID extends DoFn<String, String>{
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

    public static class JsonToBigQuery extends DoFn<String, TableRow> {
    	@ProcessElement
	public void processElement(ProcessContext data) {
		TableRow row;
		try (InputStream input = new ByteArrayInputStream(data.element().getBytes(StandardCharsets.UTF_8))) {
		    row = TableRowJsonCoder.of().decode(input, Context.OUTER);
		} catch(IOException ex){
		    throw new RuntimeException("Failed to Serialize json"+ data, ex);
		}
		data.output(row);
	}
    }
    

    public static void main(String[] args) {
        PipelineOptions pop = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(pop);
	String TargetTable = "SELECT Order1.Order_ID, Order_Date, Ship_Date, Ship_Mode, Customer_ID, Customer_Name, Segment, City, State, Country, Postal_Code, Market, Region, Product_ID, Category, Sub_Category, Product_Name, Sales, Quantity, Discount, Profit, Shipping_Cost, Order_Priority,string_field_0 as People,STRING_AGG(ModePriority) as ModePriority FROM Test.Orders_Fuck as Order1 INNER JOIN Test.People_Fuck as People1 ON Region=string_field_1 INNER JOIN Test.Mode_Fuck as Mode1 On Order1.Order_ID=Mode1.Order_ID GROUP BY Order1.Order_ID,Order_Date, Ship_Date, Ship_Mode, Customer_ID, Customer_Name, Segment, City, State, Country, Postal_Code, Market, Region, Product_ID, Category, Sub_Category, Product_Name, Sales, Quantity, Discount, Profit, Shipping_Cost, Order_Priority,string_field_0;";

	List<TableFieldSchema> fields = new ArrayList<>();
	fields.add(new TableFieldSchema().setName("Order_Date").setType("DATE"));
	fields.add(new TableFieldSchema().setName("Ship_Date").setType("DATE"));
	fields.add(new TableFieldSchema().setName("Ship_Mode").setType("STRING"));
	fields.add(new TableFieldSchema().setName("Customer_ID").setType("STRING"));
	fields.add(new TableFieldSchema().setName("Customer_Name").setType("STRING"));
	fields.add(new TableFieldSchema().setName("Segment").setType("STRING"));
	fields.add(new TableFieldSchema().setName("City").setType("STRING"));
	fields.add(new TableFieldSchema().setName("State").setType("STRING"));
	fields.add(new TableFieldSchema().setName("Country").setType("STRING"));
	fields.add(new TableFieldSchema().setName("Market").setType("STRING"));
	fields.add(new TableFieldSchema().setName("Region").setType("STRING"));
	fields.add(new TableFieldSchema().setName("Category").setType("STRING"));
	fields.add(new TableFieldSchema().setName("Sub_Category").setType("STRING"));
	fields.add(new TableFieldSchema().setName("Product_Name").setType("STRING"));
	fields.add(new TableFieldSchema().setName("Sales").setType("FLOAT"));
	fields.add(new TableFieldSchema().setName("Postal_Code").setType("STRING"));
	fields.add(new TableFieldSchema().setName("Quantity").setType("INTEGER"));
	fields.add(new TableFieldSchema().setName("Discount").setType("FLOAT"));
	fields.add(new TableFieldSchema().setName("Profit").setType("FLOAT"));
	fields.add(new TableFieldSchema().setName("Shipping_Cost").setType("FLOAT"));
	fields.add(new TableFieldSchema().setName("Order_Priority").setType("STRING"));
	fields.add(new TableFieldSchema().setName("People").setType("STRING"));
	fields.add(new TableFieldSchema().setName("OrderType").setType("STRING"));
	fields.add(new TableFieldSchema().setName("OrderYear").setType("INTEGER"));
	fields.add(new TableFieldSchema().setName("OrderNumber").setType("INTEGER"));
	fields.add(new TableFieldSchema().setName("CategoryID").setType("STRING"));
	fields.add(new TableFieldSchema().setName("SubCategoryID").setType("STRING"));
	fields.add(new TableFieldSchema().setName("ProductNumber").setType("INTEGER"));
	fields.add(new TableFieldSchema().setName("Sub_Priority").setType("STRING"));
	fields.add(new TableFieldSchema().setName("Sub_Mode").setType("STRING"));
	fields.add(new TableFieldSchema().setName("Cost").setType("FLOAT"));
	fields.add(new TableFieldSchema().setName("Margin").setType("FLOAT"));

	TableSchema schema = new TableSchema().setFields(fields);



        PCollection<String> CombinedFiles = pipeline
		.apply("Read from BQ", BigQueryIO.readTableRows().fromQuery(TargetTable).usingStandardSql())
		.apply("Tablerow to Json", AsJsons.of(TableRow.class));

	CombinedFiles
		.apply("Split ProductID", ParDo.of(new splitProductID()))
		.apply("Split OrderID", ParDo.of(new splitOrderID()))
		.apply("Split ModePriority", ParDo.of(new splitModePriority()))
		.apply("Calculate Margin and Cost", ParDo.of(new calculateCost()))
		.apply("Converting back to Bigquery Compatible", ParDo.of(new JsonToBigQuery()))
		.apply("Writing to Bigquery", BigQueryIO.writeTableRows()
				.to("fuck-260904:Test.Test_Result")
				.withSchema(schema)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        pipeline.run();
    }
    static PCollection<String> applyTransforms(
            PCollection<String> orders, PCollection<String> people){
            return PCollectionList.of(orders).and(people)
                    .apply("FFFF" ,Flatten.pCollections());

    }
}
