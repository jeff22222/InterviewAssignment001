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
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import test.test.test1.EnterFile.FormatForBigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;

public class AnotherEnterFile {
	private static final Logger LOG = LoggerFactory.getLogger(AnotherEnterFile.class);
    public static class StringToRowConverter extends DoFn<String, TableRow> {
  
        private String[] columnNames;

        private boolean isFirstRow = true;

        public void processElement(ProcessContext c) {
            TableRow row = new TableRow();

            String[] parts = c.element().split(",");

            if (isFirstRow) {
                columnNames = Arrays.copyOf(parts, parts.length);
                isFirstRow = false;
            } else {
                for (int i = 0; i < parts.length; i++) {
                    row.set(columnNames[i], parts[i]);
                }
                c.output(row);
            }
        }
    }

    public static void main(String[] args) throws Throwable {
        String sourceFilePathOrder = "gs://fuckfile/Fuck/Orders.csv";
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
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
                BigQueryIO.writeTableRows().to("fuck-260904:Test.Orders_FuckTest")
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

          p.run().waitUntilFinish();
    }
}