
package org.apache.beam.examples;

import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

//import org.apache.beam.examples.snippets.transforms.io.gcp.bigquery.BigQueryMyData.MyData;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.values.TypeDescriptor;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.metrics.MetricsEnvironment;

public class UsaNamesCount {

    public interface NameCountOptions extends PipelineOptions {

        /*
         * @Description("Path of the file to read from")
         * 
         * @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
         * String getInputFile();
         * void setInputFile(String value);
         */

        @Description("Path of the file to write to")
        @Default.String("gs://minamoto_dataflow/output.txt")
        @Required
        String getOutput();

        void setOutput(String value);
    }

    static class NameCountsFn extends DoFn<TableRow, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row = c.element();
            String alphabet = (String) row.get("name");
            //
            if (alphabet.startsWith("M")) {
                c.output((String) row.get("state"));
            }
        }
    }

    static class NameCountsWithState extends PTransform<PCollection<TableRow>, PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<TableRow> row) {
            PCollection<String> FilteredState = row.apply(ParDo.of(new NameCountsFn()));
            PCollection<KV<String, Long>> NameCounts = FilteredState.apply(Count.perElement());
            return NameCounts;
        }
    }

    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }

    static void runNameCount(NameCountOptions options) {
        Pipeline p = Pipeline.create(options);

        // p.apply("ReadLines", TextIO.read().from(options.getInputFile()));
        // 以下と意味は同じ
        // p.apply("ReadLines",
        // TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"));

        String project = "bigquery-public-data";
        String dataset = "usa_names";
        String table = "usa_1910_current";
        PCollection<TableRow> GettingTable = p.apply("ReadLines",
                BigQueryIO.readTableRows().from(String.format("%s:%s.%s", project, dataset, table)));

        GettingTable.apply(ParDo.of(new NameCountsFn()))
                .apply(Count.perElement())
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteCounts", TextIO.write().to(options.getOutput()));

        p.run().waitUntilFinish();
        System.out.println("end");
    }

    public static void main(String[] args) {
        MetricsEnvironment.setMetricsSupported(true);// 追加
        System.out.println("start");

        NameCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(NameCountOptions.class);

        runNameCount(options);

    }

}
