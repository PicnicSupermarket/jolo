package tech.picnic.jolo;

import static tech.picnic.jolo.Loader.toLinkedObjectsWith;
import static tech.picnic.jolo.TestUtil.createRecord;
import static tech.picnic.jolo.data.schema.base.Tables.BAR;
import static tech.picnic.jolo.data.schema.base.Tables.FOO;
import static tech.picnic.jolo.data.schema.base.Tables.FOOBAR;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jooq.Record;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

@SuppressWarnings("checkstyle:designforextension")
public class TestBenchmark {

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    @Param({"100", "1000", "100000"})
    public int numRecords;

    @SuppressWarnings("NullAway")
    List<Record> records;

    @SuppressWarnings("NullAway")
    Loader<TestUtil.FooEntity> loader;

    @Setup(Level.Trial)
    public void initializeLoader() {
      Entity<TestUtil.FooEntity, ?> foo = new Entity<>(FOO, TestUtil.FooEntity.class);
      Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);
      loader =
          Loader.of(foo)
              .manyToMany(foo, bar, FOOBAR)
              .setManyLeft(TestUtil.FooEntity::setBarList)
              .setManyRight(TestUtil.BarEntity::setFooList)
              .build();
    }

    @Setup(Level.Iteration)
    public void generateRecords() {
      Random random = new Random(42);
      records =
          Stream.generate(
                  () -> {
                    int foo = random.nextInt(numRecords / 2);
                    int bar = random.nextInt(numRecords / 2);
                    return createRecord(
                        ImmutableMap.of(
                            FOO.ID, (long) foo,
                            FOO.FOO_, foo,
                            BAR.ID, (long) bar,
                            BAR.FOOID, (long) foo,
                            BAR.BAR_, bar,
                            FOOBAR.FOOID, (long) foo,
                            FOOBAR.BARID, (long) bar));
                  })
              .limit(numRecords)
              .collect(Collectors.toUnmodifiableList());
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @Fork(warmups = 1, value = 1)
  @Warmup(iterations = 2, time = 1)
  @Measurement(iterations = 20, time = 1)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void benchmark(BenchmarkState state, Blackhole hole) {
    List<TestUtil.FooEntity> objects =
        state.records.stream().collect(toLinkedObjectsWith(state.loader));
    hole.consume(objects);
  }

  public static void main(String[] args) throws Exception {
    Options options =
        new OptionsBuilder()
            .verbosity(VerboseMode.EXTRA)
            .addProfiler("jfr")
            .include(TestBenchmark.class.getSimpleName())
            .build();
    new Runner(options).run();
  }
}
