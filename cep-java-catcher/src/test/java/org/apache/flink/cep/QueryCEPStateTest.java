package org.apache.flink.cep;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.entity.Page;
import org.apache.flink.cep.event.EventWrapper;
import org.apache.flink.cep.nfa.NFAState;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.state.PageSourceFunction;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.test.util.MiniClusterResourceConfiguration;
import org.apache.flink.util.TestLogger;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class QueryCEPStateTest extends TestLogger {

//    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final int NUM_TMS = 2;

    private static final int QS_PROXY_PORT_RANGE_START = 9094;
    private static final int QS_SERVER_PORT_RANGE_START = 9099;

    private static QueryableStateClient client;

    @ClassRule
    public static final MiniClusterResource miniClusterResource = new MiniClusterResource(miniClusterResourceConfiguration());

    private static ClusterClient clusterClient;

    private static StateBackend stateBackend;

    private String simplePattern = "myblog";

    private MapStateDescriptor<String, NFAState> patternStateMapDesc;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @BeforeClass
    public static void setup() throws IOException {
        client = new QueryableStateClient("localhost", QS_PROXY_PORT_RANGE_START);
        clusterClient = miniClusterResource.getClusterClient();
    }

    @Before
    public void before() {
        this.patternStateMapDesc = new MapStateDescriptor<>("cep",
                BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(NFAState.class));
    }

    @AfterClass
    public static void afterClass() {
        miniClusterResource.after();
    }

    @Test
    public void testStateQuery() throws ProgramInvocationException, IOException, InterruptedException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        stateBackend = new FsStateBackend(temporaryFolder.newFolder().toURI().toString());
        env.setStateBackend(stateBackend);

        client.setExecutionConfig(env.getConfig());

        DataStream<EventWrapper<Page>> input = env.addSource(new PageSourceFunction(simplePattern))
                .keyBy((KeySelector<EventWrapper<Page>, String>) EventWrapper::getPattern);

        List<Pattern> patterns = buildPatterns();

        CEP.pattern(input, patterns)
                .select((PatternSelectFunction<String>)
                        matchingUser -> matchingUser.f0 + "," + matchingUser.f1.toString());

        // submit job
        JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        JobID jobId = jobGraph.getJobID();

        clusterClient.setDetached(true);
        clusterClient.submitJob(jobGraph, this.getClass().getClassLoader());

        AtomicBoolean result = new AtomicBoolean(false);

        final Deadline deadline = Deadline.now().plus(Duration.ofSeconds(60));

        List<Future<?>> futures = new ArrayList<>();
        while (!result.get() && deadline.hasTimeLeft()) {
            CompletableFuture<MapState<String, NFAState>> future = client.getKvState(jobId, "cep",
                    simplePattern, BasicTypeInfo.STRING_TYPE_INFO, patternStateMapDesc);
            CompletableFuture<MapState<String, NFAState>> completeFuture = future.whenComplete((state, throwable) -> {
                if (throwable != null) {
//                    log.error("Fail to query state: " + throwable.getLocalizedMessage().substring(0, 1024));
                } else {
                    log.info("Find state.");
                    updateResult(state, result);
                }
            });
            Thread.sleep(1000);
            futures.add(completeFuture);
        }

        Assert.assertEquals(true, result.get());
    }

    private void updateResult(MapState<String, NFAState> state, AtomicBoolean result) {
        try {
            RoaringBitmap bm = state.get(simplePattern).getPartialMatches().entrySet().stream()
                    .map(Map.Entry::getValue).reduce((x1, x2) -> RoaringBitmap.or(x1, x2)).orElse(new RoaringBitmap());
            if (bm.getLongCardinality() > 0) {
                result.set(true);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<Pattern> buildPatterns() {
        List<Pattern> patterns = new ArrayList<>();

        patterns.add(simplePattern());
        return patterns;
    }

    private Pattern simplePattern() {
        Pattern pattern = Pattern.begin("register").where(new IterativeCondition() {
            @Override
            public boolean filter(EventWrapper value, Context ctx) throws Exception {
                Page page = (Page)value.getEvent();
                return page.getUrl().equals("/register");
            }
        }).followedBy("login").where(new IterativeCondition() {
            @Override
            public boolean filter(EventWrapper value, Context ctx) throws Exception {
                Page page = (Page)value.getEvent();
                return page.getUrl().equals("/login");
            }
        }).followedBy("purchase").where(new IterativeCondition() {
            @Override
            public boolean filter(EventWrapper value, Context ctx) throws Exception {
                Page page = (Page)value.getEvent();
                return page.getUrl().equals("/purchase");
            }
        });
        pattern.setId(simplePattern);
        return pattern;
    }

    public static MiniClusterResourceConfiguration miniClusterResourceConfiguration() {
        Configuration config = new Configuration();
        config.setString(
                QueryableStateOptions.PROXY_PORT_RANGE,
                QS_PROXY_PORT_RANGE_START + "-" + (QS_PROXY_PORT_RANGE_START + NUM_TMS));
        config.setInteger(QueryableStateOptions.CLIENT_NETWORK_THREADS, 1);
        config.setInteger(QueryableStateOptions.PROXY_NETWORK_THREADS, 1);
        config.setInteger(QueryableStateOptions.SERVER_NETWORK_THREADS, 1);
        config.setString(
                QueryableStateOptions.SERVER_PORT_RANGE,
                QS_SERVER_PORT_RANGE_START + "-" + (QS_SERVER_PORT_RANGE_START + NUM_TMS));
        return new MiniClusterResourceConfiguration.Builder()
                .setConfiguration(config)
                .setNumberSlotsPerTaskManager(1)
                .setNumberTaskManagers(2)
                .build();
    }

}
