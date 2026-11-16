package com.fyp;

import java.nio.file.Path;

import com.fyp.config.AppConfig;
import com.fyp.config.ConfigLoader;
import com.fyp.model.WindowOutput;
import com.fyp.service.AttributionService;
import com.fyp.storage.RunResults;

/**
 * Command line entry point: loads the config, runs the pipeline over the configured dataset, then
 * reads the results back using only the run id, exactly as a caller such as the dashboard would.
 *
 * <p>Run from {@code backend/} with {@code mvn compile exec:exec "-DmainClass=com.fyp.AttributionJob"}.
 */
public final class AttributionJob {

    private AttributionJob() {
    }

    public static void main(String[] args) throws Exception {
        AppConfig config = ConfigLoader.load();
        Path datasetCsv = Path.of(config.dataset().resolveTarget().file());

        try (AttributionService service = AttributionService.fromConfig(config)) {
            String runId = service.run(datasetCsv);
            RunResults results = service.fetchAll(runId);

            System.out.println(runId + ": " + results.outputs().size() + " window outputs, "
                    + results.totalCounted() + " hashtag events counted from " + datasetCsv.getFileName());
            for (WindowOutput output : results.outputs()) {
                System.out.printf("[%d,%d) %-10s count=%-4d %s%n", output.windowStart(),
                        output.windowEnd(), output.category(), output.outputCount(),
                        output.attribution().toJson());
            }
        }
    }
}
