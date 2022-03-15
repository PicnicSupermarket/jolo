package io.github.ferdinandswoboda.folo;

import static io.github.ferdinandswoboda.folo.TestUtil.createRecord;
import static io.github.ferdinandswoboda.folo.data.schema.base.Tables.BAR;
import static io.github.ferdinandswoboda.folo.data.schema.base.Tables.BAZ;
import static io.github.ferdinandswoboda.folo.data.schema.base.Tables.FOO;
import static io.github.ferdinandswoboda.folo.data.schema.base.Tables.FOOBAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.github.ferdinandswoboda.folo.data.schema.base.tables.Bar;
import io.github.ferdinandswoboda.folo.data.schema.base.tables.Foo;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;

public final class LoaderTest {
  @Test
  public void testOneToMany() {
    runOneToManyTest(FOO, BAR);
  }

  @Test
  public void testOneToManyAliased() {
    runOneToManyTest(FOO.as("BAR"), BAR.as("BAZ"));
  }

  private static void runOneToManyTest(Foo fooTable, Bar barTable) {
    Entity<TestUtil.FooEntity, ?> foo = new Entity<>(fooTable, TestUtil.FooEntity.class);
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(barTable, TestUtil.BarEntity.class);
    Loader<TestUtil.FooEntity> l =
        Loader.of(foo)
            .oneToMany(foo, bar)
            .setManyLeft(TestUtil.FooEntity::setBarList)
            .setOneRight(TestUtil.BarEntity::setFoo)
            .build();

    ObjectGraph objectGraph = l.supplier().get();
    BiConsumer<ObjectGraph, Record> accumulator = l.accumulator();

    accumulator.accept(
        objectGraph,
        createRecord(
            ImmutableMap.of(
                fooTable.ID, 1L,
                fooTable.FOO_, 1,
                barTable.ID, 1L,
                barTable.FOOID, 1L,
                barTable.BAR_, 2)));
    accumulator.accept(
        objectGraph,
        createRecord(
            ImmutableMap.of(
                fooTable.ID, 1L,
                fooTable.FOO_, 1,
                barTable.ID, 2L,
                barTable.FOOID, 1L,
                barTable.BAR_, 3)));

    TestUtil.FooEntity expectedFoo = new TestUtil.FooEntity(1L, 1, null);
    TestUtil.BarEntity expectedBar1 = new TestUtil.BarEntity(1L, 1L, 2, null, null);
    TestUtil.BarEntity expectedBar2 = new TestUtil.BarEntity(2L, 1L, 3, null, null);
    expectedBar1.setFoo(expectedFoo);
    expectedBar2.setFoo(expectedFoo);
    expectedFoo.setBarList(ImmutableList.of(expectedBar1, expectedBar2));

    List<TestUtil.FooEntity> entities = l.finisher().apply(objectGraph);
    assertIterableEquals(ImmutableList.of(expectedFoo), entities);
  }

  @Test
  public void testOneToManyWrongWayAround() {
    Entity<TestUtil.FooEntity, ?> foo = new Entity<>(FOO, TestUtil.FooEntity.class);
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);
    assertThrows(IllegalArgumentException.class, () -> Loader.of(foo).oneToMany(bar, foo));
  }

  @Test
  public void testEncounterOrder() {
    Entity<TestUtil.FooEntity, ?> foo = new Entity<>(FOO, TestUtil.FooEntity.class);
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);
    Loader<TestUtil.BarEntity> l =
        Loader.of(bar).oneToOne(bar, foo).setOneLeft(TestUtil.BarEntity::setFoo).build();

    ObjectGraph objectGraph = l.supplier().get();
    BiConsumer<ObjectGraph, Record> accumulator = l.accumulator();

    accumulator.accept(
        objectGraph,
        createRecord(
            ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1, BAR.ID, 1L, BAR.FOOID, 1L, BAR.BAR_, 2)));
    accumulator.accept(
        objectGraph,
        createRecord(
            ImmutableMap.of(FOO.ID, 3L, FOO.FOO_, 1, BAR.ID, 3L, BAR.FOOID, 3L, BAR.BAR_, 2)));
    accumulator.accept(
        objectGraph,
        createRecord(
            ImmutableMap.of(FOO.ID, 2L, FOO.FOO_, 1, BAR.ID, 2L, BAR.FOOID, 2L, BAR.BAR_, 2)));

    TestUtil.FooEntity expectedFoo = new TestUtil.FooEntity(1L, 1, null);
    TestUtil.FooEntity expectedFoo2 = new TestUtil.FooEntity(2L, 1, null);
    TestUtil.FooEntity expectedFoo3 = new TestUtil.FooEntity(3L, 1, null);
    TestUtil.BarEntity expectedBar = new TestUtil.BarEntity(1L, 1L, 2, null, null);
    expectedBar.setFoo(expectedFoo);
    TestUtil.BarEntity expectedBar2 = new TestUtil.BarEntity(2L, 2L, 2, null, null);
    expectedBar2.setFoo(expectedFoo2);
    TestUtil.BarEntity expectedBar3 = new TestUtil.BarEntity(3L, 3L, 2, null, null);
    expectedBar3.setFoo(expectedFoo3);

    List<TestUtil.BarEntity> entities = l.finisher().apply(objectGraph);
    assertIterableEquals(ImmutableList.of(expectedBar, expectedBar3, expectedBar2), entities);
  }

  @Test
  public void testOneToOne() {
    Entity<TestUtil.FooEntity, ?> foo = new Entity<>(FOO, TestUtil.FooEntity.class);
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);
    Loader<TestUtil.BarEntity> l =
        Loader.of(bar).oneToOne(bar, foo).setOneLeft(TestUtil.BarEntity::setFoo).build();

    ObjectGraph objectGraph = l.supplier().get();
    BiConsumer<ObjectGraph, Record> accumulator = l.accumulator();

    // Add the same record twice to check that there are no duplicates
    accumulator.accept(
        objectGraph,
        createRecord(
            ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1, BAR.ID, 1L, BAR.FOOID, 1L, BAR.BAR_, 2)));
    accumulator.accept(
        objectGraph,
        createRecord(
            ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1, BAR.ID, 1L, BAR.FOOID, 1L, BAR.BAR_, 2)));

    TestUtil.FooEntity expectedFoo = new TestUtil.FooEntity(1L, 1, null);
    TestUtil.BarEntity expectedBar = new TestUtil.BarEntity(1L, 1L, 2, null, null);
    expectedBar.setFoo(expectedFoo);

    List<TestUtil.BarEntity> entities = l.finisher().apply(objectGraph);
    assertIterableEquals(ImmutableList.of(expectedBar), entities);
  }

  @Test
  public void testOneToOneAmbiguous() {
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);
    Entity<TestUtil.BazEntity, ?> baz = new Entity<>(BAZ, TestUtil.BazEntity.class);
    assertThrows(ValidationException.class, () -> Loader.of(bar).oneToOne(bar, baz));
  }

  @Test
  public void testOneToZeroOrOne() {
    Entity<TestUtil.FooEntity, ?> foo = new Entity<>(FOO, TestUtil.FooEntity.class);
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);
    Loader<TestUtil.FooEntity> l =
        Loader.of(foo)
            .oneToZeroOrOne(foo, bar)
            .setZeroOrOneLeft(TestUtil.FooEntity::setBarOptional)
            .setOneRight(TestUtil.BarEntity::setFoo)
            .build();

    ObjectGraph objectGraph = l.supplier().get();
    BiConsumer<ObjectGraph, Record> accumulator = l.accumulator();

    accumulator.accept(
        objectGraph,
        createRecord(
            ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1, BAR.ID, 1L, BAR.FOOID, 1L, BAR.BAR_, 2)));
    accumulator.accept(objectGraph, createRecord(ImmutableMap.of(FOO.ID, 2L, FOO.FOO_, 2), BAR));

    TestUtil.FooEntity expectedFoo1 = new TestUtil.FooEntity(1L, 1, null);
    TestUtil.FooEntity expectedFoo2 = new TestUtil.FooEntity(2L, 2, null);
    TestUtil.BarEntity expectedBar = new TestUtil.BarEntity(1L, 1L, 2, null, null);
    expectedBar.setFoo(expectedFoo1);
    expectedFoo1.setBarOptional(Optional.of(expectedBar));
    expectedFoo2.setBarOptional(Optional.empty());

    assertIterableEquals(
        ImmutableList.of(expectedFoo1, expectedFoo2), l.finisher().apply(objectGraph));
  }

  @Test
  public void testOptionalOneToOne() {
    Entity<TestUtil.FooEntity, ?> foo = new Entity<>(FOO, TestUtil.FooEntity.class);
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);
    Loader<TestUtil.BarEntity> l =
        Loader.of(bar)
            .optionalOneToOne(bar, foo)
            .setZeroOrOneLeft(TestUtil.BarEntity::setFooOptional)
            .build();

    ObjectGraph objectGraph = l.supplier().get();
    BiConsumer<ObjectGraph, Record> accumulator = l.accumulator();

    // Add the same record twice to check that there are no duplicates
    accumulator.accept(
        objectGraph,
        createRecord(
            ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1, BAR.ID, 1L, BAR.FOOID, 1L, BAR.BAR_, 2)));
    accumulator.accept(
        objectGraph,
        createRecord(
            ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1, BAR.ID, 1L, BAR.FOOID, 1L, BAR.BAR_, 2)));

    TestUtil.FooEntity expectedFoo = new TestUtil.FooEntity(1L, 1, null);
    TestUtil.BarEntity expectedBar = new TestUtil.BarEntity(1L, 1L, 2, null, null);
    expectedBar.setFooOptional(Optional.of(expectedFoo));

    assertIterableEquals(ImmutableList.of(expectedBar), l.finisher().apply(objectGraph));
  }

  @Test
  public void testEmptyOptionalOneToOne() {
    Entity<TestUtil.FooEntity, ?> foo = new Entity<>(FOO, TestUtil.FooEntity.class);
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);
    Loader<TestUtil.BarEntity> l =
        Loader.of(bar)
            .optionalOneToOne(bar, foo)
            .setZeroOrOneLeft(TestUtil.BarEntity::setFooOptional)
            .build();

    ObjectGraph objectGraph = l.supplier().get();
    BiConsumer<ObjectGraph, Record> accumulator = l.accumulator();

    // Add the same record twice to check that there are no duplicates
    accumulator.accept(
        objectGraph, createRecord(ImmutableMap.of(BAR.ID, 1L, BAR.BAR_, 2), FOO, BAR));
    accumulator.accept(
        objectGraph, createRecord(ImmutableMap.of(BAR.ID, 1L, BAR.BAR_, 2), FOO, BAR));

    TestUtil.BarEntity expectedBar = new TestUtil.BarEntity(1L, null, 2, null, null);
    expectedBar.setFooOptional(Optional.empty());

    assertIterableEquals(ImmutableList.of(expectedBar), l.finisher().apply(objectGraph));
  }

  @Test
  public void testZeroOrOneToOne() {
    Entity<TestUtil.FooEntity, ?> foo = new Entity<>(FOO, TestUtil.FooEntity.class);
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);
    Loader<TestUtil.BarEntity> l =
        Loader.of(bar)
            .optionalOneToOne(bar, foo)
            .setZeroOrOneLeft(TestUtil.BarEntity::setFooOptional)
            .build();

    ObjectGraph objectGraph = l.supplier().get();
    BiConsumer<ObjectGraph, Record> accumulator = l.accumulator();

    accumulator.accept(objectGraph, createRecord(ImmutableMap.of(BAR.ID, 1L, BAR.BAR_, 2), FOO));

    TestUtil.BarEntity expectedBar = new TestUtil.BarEntity(1L, null, 2, null, null);
    expectedBar.setFooOptional(Optional.empty());

    assertIterableEquals(ImmutableList.of(expectedBar), l.finisher().apply(objectGraph));
  }

  @Test
  public void testZeroOrOneToMany() {
    Entity<TestUtil.FooEntity, ?> foo = new Entity<>(FOO, TestUtil.FooEntity.class);
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);
    Loader<TestUtil.BarEntity> l =
        Loader.of(bar)
            .zeroOrOneToMany(foo, bar)
            .setManyLeft(TestUtil.FooEntity::setBarList)
            .setZeroOrOneRight(TestUtil.BarEntity::setFooOptional)
            .build();

    ObjectGraph objectGraph = l.supplier().get();
    BiConsumer<ObjectGraph, Record> accumulator = l.accumulator();

    accumulator.accept(
        objectGraph,
        createRecord(
            ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1, BAR.ID, 1L, BAR.FOOID, 1L, BAR.BAR_, 2)));
    accumulator.accept(
        objectGraph,
        createRecord(ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1, BAR.ID, 2L, BAR.BAR_, 3)));

    TestUtil.FooEntity expectedFoo = new TestUtil.FooEntity(1L, 1, null);
    TestUtil.BarEntity expectedBar1 = new TestUtil.BarEntity(1L, 1L, 2, null, null);
    TestUtil.BarEntity expectedBar2 = new TestUtil.BarEntity(2L, null, 3, null, null);
    expectedBar1.setFooOptional(Optional.of(expectedFoo));
    expectedBar2.setFooOptional(Optional.empty());
    expectedFoo.setBarList(ImmutableList.of(expectedBar1));

    assertIterableEquals(
        ImmutableSet.of(expectedBar1, expectedBar2), l.finisher().apply(objectGraph));
  }

  @Test
  public void testZeroOrOneToManyRecursiveReference() {
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);
    Loader<TestUtil.BarEntity> l =
        Loader.of(bar)
            .zeroOrOneToMany(bar, bar)
            .setZeroOrOneRight(TestUtil.BarEntity::setOtherBar)
            .build();

    ObjectGraph objectGraph = l.supplier().get();
    BiConsumer<ObjectGraph, Record> accumulator = l.accumulator();

    accumulator.accept(
        objectGraph, createRecord(ImmutableMap.of(BAR.ID, 1L, BAR.BAR_, 1, BAR.OTHERBARID, 2L)));
    accumulator.accept(
        objectGraph, createRecord(ImmutableMap.of(BAR.ID, 2L, BAR.BAR_, 2, BAR.OTHERBARID, 2L)));

    TestUtil.BarEntity expectedBar1 = new TestUtil.BarEntity(1L, null, 1, 2L, null);
    TestUtil.BarEntity expectedBar2 = new TestUtil.BarEntity(2L, null, 2, 2L, null);
    expectedBar1.setOtherBar(Optional.of(expectedBar2));
    expectedBar2.setOtherBar(Optional.of(expectedBar2));

    assertIterableEquals(
        ImmutableList.of(expectedBar1, expectedBar2), l.finisher().apply(objectGraph));
  }

  @Test
  public void testNToOneThrowsIfManyAreFound() {
    Entity<TestUtil.FooEntity, ?> foo = new Entity<>(FOO, TestUtil.FooEntity.class);
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);
    Loader<TestUtil.BarEntity> l =
        Loader.of(bar).oneToOne(bar, foo).setOneLeft(TestUtil.BarEntity::setFoo).build();

    ObjectGraph objectGraph = l.supplier().get();
    BiConsumer<ObjectGraph, Record> accumulator = l.accumulator();

    accumulator.accept(
        objectGraph,
        createRecord(
            ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1, BAR.ID, 1L, BAR.FOOID, 1L, BAR.BAR_, 2)));
    accumulator.accept(
        objectGraph,
        createRecord(
            ImmutableMap.of(FOO.ID, 2L, FOO.FOO_, 2, BAR.ID, 1L, BAR.FOOID, 2L, BAR.BAR_, 2)));

    TestUtil.FooEntity expectedFoo = new TestUtil.FooEntity(1L, 1, null);
    TestUtil.BarEntity expectedBar = new TestUtil.BarEntity(1L, 1L, 2, null, null);
    expectedBar.setFoo(expectedFoo);

    assertThrows(ValidationException.class, () -> l.finisher().apply(objectGraph));
  }

  @Test
  public void testManyToMany() {
    Entity<TestUtil.FooEntity, ?> foo = new Entity<>(FOO, TestUtil.FooEntity.class);
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);
    Loader<TestUtil.FooEntity> l =
        Loader.of(foo)
            .manyToMany(foo, bar, FOOBAR)
            .setManyLeft(TestUtil.FooEntity::setBarList)
            .setManyRight(TestUtil.BarEntity::setFooList)
            .build();

    ObjectGraph objectGraph = l.supplier().get();
    BiConsumer<ObjectGraph, Record> accumulator = l.accumulator();

    accumulator.accept(
        objectGraph,
        createRecord(
            ImmutableMap.<Field<?>, Object>builder()
                .put(FOO.ID, 1L)
                .put(FOO.FOO_, 1)
                .put(BAR.ID, 1L)
                .put(BAR.FOOID, 1L)
                .put(BAR.BAR_, 2)
                .put(FOOBAR.FOOID, 1L)
                .put(FOOBAR.BARID, 1L)
                .build()));

    TestUtil.FooEntity expectedFoo = new TestUtil.FooEntity(1L, 1, null);
    TestUtil.BarEntity expectedBar = new TestUtil.BarEntity(1L, 1L, 2, null, null);
    expectedBar.setFooList(ImmutableList.of(expectedFoo));
    expectedFoo.setBarList(ImmutableList.of(expectedBar));

    assertIterableEquals(ImmutableList.of(expectedFoo), l.finisher().apply(objectGraph));
  }

  @Test
  public void testEmpty() {
    Entity<TestUtil.FooEntity, ?> foo = new Entity<>(FOO, TestUtil.FooEntity.class);
    Loader<TestUtil.FooEntity> l = Loader.of(foo).build();

    ObjectGraph objectGraph = l.supplier().get();

    assertIterableEquals(ImmutableList.of(), l.finisher().apply(objectGraph));
  }

  @Test
  public void testRelationWithDummyRelationLoader() {
    Entity<TestUtil.FooEntity, ?> foo = new Entity<>(FOO, TestUtil.FooEntity.class);
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);

    Loader<TestUtil.FooEntity> l =
        Loader.of(foo)
            .manyToMany(foo, bar, FOOBAR)
            .setManyLeft(TestUtil.FooEntity::setBarList)
            .setManyRight(TestUtil.BarEntity::setFooList)
            .setRelationLoader(record -> Set.of(IdPair.of(1, 1)))
            .build();

    ObjectGraph objectGraph = l.supplier().get();
    BiConsumer<ObjectGraph, Record> accumulator = l.accumulator();

    accumulator.accept(
        objectGraph,
        createRecord(
            ImmutableMap.<Field<?>, Object>builder()
                .put(FOO.ID, 1L)
                .put(FOO.FOO_, 1)
                .put(BAR.ID, 1L)
                .put(BAR.FOOID, 1L)
                .put(BAR.BAR_, 2)
                .build()));

    List<TestUtil.FooEntity> entities = l.finisher().apply(objectGraph);
    assertEquals(1, entities.size());
    assertEquals(1L, entities.get(0).getBarList().get(0).getId());
  }

  @Test
  public void testRelationWithCustomRelationLoader() {
    Entity<TestUtil.FooEntity, ?> foo = new Entity<>(FOO, TestUtil.FooEntity.class);
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);

    Loader<TestUtil.FooEntity> l =
        Loader.of(foo)
            .manyToMany(foo, bar, FOOBAR)
            .setManyLeft(TestUtil.FooEntity::setBarList)
            .setManyRight(TestUtil.BarEntity::setFooList)
            .setRelationLoader(LoaderTest::customRelationLoader)
            .build();

    ObjectGraph objectGraph = l.supplier().get();
    BiConsumer<ObjectGraph, Record> accumulator = l.accumulator();

    accumulator.accept(
        objectGraph,
        createRecord(
            ImmutableMap.<Field<?>, Object>builder()
                .put(FOO.ID, 1L)
                .put(FOO.FOO_, 1)
                .put(FOO.RELATEDFOOIDS, new Long[] {2L})
                .put(BAR.ID, 1L)
                .put(BAR.FOOID, 1L)
                .put(BAR.BAR_, 2)
                .put(FOOBAR.FOOID, 1L)
                .put(FOOBAR.BARID, 1L)
                .build()));
    accumulator.accept(
        objectGraph,
        createRecord(
            ImmutableMap.<Field<?>, Object>builder()
                .put(FOO.ID, 2L)
                .put(FOO.FOO_, 2)
                .put(FOO.RELATEDFOOIDS, new Long[] {1L})
                .put(BAR.ID, 1L)
                .put(BAR.FOOID, 1L)
                .put(BAR.BAR_, 2)
                .put(FOOBAR.FOOID, 1L)
                .put(FOOBAR.BARID, 1L)
                .build()));

    List<TestUtil.FooEntity> entities = l.finisher().apply(objectGraph);
    assertEquals(2, entities.size());
    for (TestUtil.FooEntity entity : entities) {
      ImmutableList<TestUtil.BarEntity> barList = entity.getBarList();
      assertEquals(1, barList.size());
      assertEquals(1L, barList.get(0).getId());
    }
  }

  @Test
  public void testFallbackToForeignKeyRelation() {
    Entity<TestUtil.FooEntity, ?> foo = new Entity<>(FOO, TestUtil.FooEntity.class);
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);

    Loader<TestUtil.FooEntity> l =
        Loader.of(foo)
            .manyToMany(foo, bar, FOOBAR)
            .setManyLeft(TestUtil.FooEntity::setBarList)
            .setManyRight(TestUtil.BarEntity::setFooList)
            .build();

    ObjectGraph objectGraph = l.supplier().get();
    BiConsumer<ObjectGraph, Record> accumulator = l.accumulator();

    accumulator.accept(
        objectGraph,
        createRecord(
            ImmutableMap.<Field<?>, Object>builder()
                .put(FOO.ID, 1L)
                .put(FOO.FOO_, 1)
                .put(FOO.RELATEDFOOIDS, new Long[] {2L})
                .put(BAR.ID, 1L)
                .put(BAR.FOOID, 1L)
                .put(BAR.BAR_, 2)
                .put(FOOBAR.FOOID, 1L)
                .put(FOOBAR.BARID, 1L)
                .build()));
    accumulator.accept(
        objectGraph,
        createRecord(
            ImmutableMap.<Field<?>, Object>builder()
                .put(FOO.ID, 2L)
                .put(FOO.FOO_, 2)
                .put(FOO.RELATEDFOOIDS, new Long[] {1L})
                .put(BAR.ID, 1L)
                .put(BAR.FOOID, 1L)
                .put(BAR.BAR_, 2)
                .put(FOOBAR.FOOID, 1L)
                .put(FOOBAR.BARID, 1L)
                .build()));

    List<TestUtil.FooEntity> entities = l.finisher().apply(objectGraph);
    assertEquals(1L, entities.get(0).getBarList().get(0).getId());
    assertEquals(ImmutableList.of(), entities.get(1).getBarList());
  }

  @Test
  public void testLoadTwice() {
    Entity<TestUtil.FooEntity, ?> foo = new Entity<>(FOO, TestUtil.FooEntity.class);
    Loader<TestUtil.FooEntity> l = Loader.of(foo).build();

    ObjectGraph objectGraph = l.supplier().get();
    BiConsumer<ObjectGraph, Record> accumulator = l.accumulator();

    accumulator.accept(objectGraph, createRecord(ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1)));
    accumulator.accept(objectGraph, createRecord(ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 2)));

    TestUtil.FooEntity expectedFoo = new TestUtil.FooEntity(1L, 1, null);

    List<TestUtil.FooEntity> entities = l.finisher().apply(objectGraph);
    assertIterableEquals(ImmutableList.of(expectedFoo), entities);
  }

  @Test
  public void testRightFoldingCombiner() {
    Entity<TestUtil.FooEntity, ?> foo = new Entity<>(FOO, TestUtil.FooEntity.class);
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);
    Loader<TestUtil.FooEntity> l =
        Loader.of(foo)
            .manyToMany(foo, bar, FOOBAR)
            .setManyLeft(TestUtil.FooEntity::setBarList)
            .setManyRight(TestUtil.BarEntity::setFooList)
            .build();

    Supplier<ObjectGraph> supplier = l.supplier();
    BiConsumer<ObjectGraph, Record> accumulator = l.accumulator();
    BinaryOperator<ObjectGraph> combiner = l.combiner();

    Record firstRecord =
        createRecord(
            ImmutableMap.<Field<?>, Object>builder()
                .put(FOO.ID, 1L)
                .put(FOO.FOO_, 1)
                .put(BAR.ID, 1L)
                .put(BAR.BAR_, 1)
                .put(FOOBAR.FOOID, 1L)
                .put(FOOBAR.BARID, 1L)
                .build());
    Record secondRecord =
        createRecord(
            ImmutableMap.<Field<?>, Object>builder()
                .put(FOO.ID, 2L)
                .put(FOO.FOO_, 2)
                .put(BAR.ID, 2L)
                .put(BAR.BAR_, 2)
                .put(FOOBAR.FOOID, 2L)
                .put(FOOBAR.BARID, 2L)
                .build());
    Record thirdRecord =
        createRecord(
            ImmutableMap.<Field<?>, Object>builder()
                .put(FOO.ID, 1L)
                .put(FOO.FOO_, 2)
                .put(BAR.ID, 3L)
                .put(BAR.BAR_, 3)
                .put(FOOBAR.FOOID, 1L)
                .put(FOOBAR.BARID, 3L)
                .build());
    Record fourthRecord =
        createRecord(
            ImmutableMap.<Field<?>, Object>builder()
                .put(FOO.ID, 2L)
                .put(FOO.FOO_, 3)
                .put(BAR.ID, 4L)
                .put(BAR.BAR_, 4)
                .put(FOOBAR.FOOID, 2L)
                .put(FOOBAR.BARID, 4L)
                .build());

    ObjectGraph firstRight = supplier.get();
    accumulator.accept(firstRight, firstRecord);

    ObjectGraph secondRight = supplier.get();
    accumulator.accept(secondRight, secondRecord);
    accumulator.accept(secondRight, thirdRecord);

    ObjectGraph thirdRight = supplier.get();
    accumulator.accept(thirdRight, fourthRecord);

    List<TestUtil.FooEntity> entitiesRightFolded =
        l.finisher().apply(combiner.apply(firstRight, combiner.apply(secondRight, thirdRight)));

    ObjectGraph firstLeft = supplier.get();
    accumulator.accept(firstLeft, firstRecord);

    ObjectGraph secondLeft = supplier.get();
    accumulator.accept(secondLeft, secondRecord);
    accumulator.accept(secondLeft, thirdRecord);

    ObjectGraph thirdLeft = supplier.get();
    accumulator.accept(thirdLeft, fourthRecord);

    List<TestUtil.FooEntity> entitiesLeftFolded =
        l.finisher().apply(combiner.apply(combiner.apply(firstLeft, secondLeft), thirdLeft));

    TestUtil.FooEntity expectedFoo1 = new TestUtil.FooEntity(1L, 1, null);
    TestUtil.FooEntity expectedFoo2 = new TestUtil.FooEntity(2L, 2, null);
    TestUtil.BarEntity expectedBar1 = new TestUtil.BarEntity(1L, null, 1, null, null);
    TestUtil.BarEntity expectedBar2 = new TestUtil.BarEntity(2L, null, 2, null, null);
    TestUtil.BarEntity expectedBar3 = new TestUtil.BarEntity(3L, null, 3, null, null);
    TestUtil.BarEntity expectedBar4 = new TestUtil.BarEntity(4L, null, 4, null, null);
    expectedFoo1.setBarList(ImmutableList.of(expectedBar1, expectedBar3));
    expectedFoo2.setBarList(ImmutableList.of(expectedBar2, expectedBar4));
    expectedBar1.setFooList(ImmutableList.of(expectedFoo1));
    expectedBar2.setFooList(ImmutableList.of(expectedFoo2));
    expectedBar3.setFooList(ImmutableList.of(expectedFoo1));
    expectedBar4.setFooList(ImmutableList.of(expectedFoo2));

    assertIterableEquals(ImmutableList.of(expectedFoo1, expectedFoo2), entitiesRightFolded);
    assertIterableEquals(ImmutableList.of(expectedFoo1, expectedFoo2), entitiesLeftFolded);
  }

  // Collector contract tests

  /**
   * From the {@link java.util.stream.Collector} interface:
   *
   * <p>The identity constraint says that for any partially accumulated result, combining it with an
   * empty result container must produce an equivalent result. That is, for a partially accumulated
   * result {@code a} that is the result of any series of accumulator and combiner invocations,
   * {@code a} must be equivalent to {@code combiner.apply(a, supplier.get())}.
   */
  @Test
  public void testCollectorIdentity() {
    Entity<TestUtil.FooEntity, ?> foo = new Entity<>(FOO, TestUtil.FooEntity.class);
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);
    Loader<TestUtil.BarEntity> l =
        Loader.of(bar)
            .optionalOneToOne(bar, foo)
            .setZeroOrOneLeft(TestUtil.BarEntity::setFooOptional)
            .build();

    Supplier<ObjectGraph> supplier = l.supplier();
    BiConsumer<ObjectGraph, Record> accumulator = l.accumulator();
    BinaryOperator<ObjectGraph> combiner = l.combiner();
    Function<ObjectGraph, List<TestUtil.BarEntity>> finisher = l.finisher();

    ObjectGraph a = supplier.get();
    accumulator.accept(a, createRecord(ImmutableMap.of(BAR.ID, 1L, BAR.BAR_, 2), FOO));

    // test combiner identity
    assertEquals(a, combiner.apply(a, supplier.get()));
    // test finisher on identity
    assertEquals(ImmutableList.of(), finisher.apply(supplier.get()));
  }

  /**
   * From the {@link java.util.stream.Collector} interface:
   *
   * <p>The associativity constraint says that splitting the computation must produce an equivalent
   * result. That is, for any input elements {@code t1} and {@code t2}, the results {@code r1} and
   * {@code r2} in the computation below must be equivalent:
   *
   * <pre>{@code
   * A a1 = supplier.get();
   * accumulator.accept(a1, t1);
   * accumulator.accept(a1, t2);
   * R r1 = finisher.apply(a1);  // result without splitting
   *
   * A a2 = supplier.get();
   * accumulator.accept(a2, t1);
   * A a3 = supplier.get();
   * accumulator.accept(a3, t2);
   * R r2 = finisher.apply(combiner.apply(a2, a3));  // result with splitting
   * }</pre>
   */
  @Test
  public void testCombinerAssociativity() {
    Entity<TestUtil.FooEntity, ?> foo = new Entity<>(FOO, TestUtil.FooEntity.class);
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);
    Loader<TestUtil.BarEntity> l =
        Loader.of(bar)
            .optionalOneToOne(bar, foo)
            .setZeroOrOneLeft(TestUtil.BarEntity::setFooOptional)
            .build();

    Supplier<ObjectGraph> supplier = l.supplier();
    BiConsumer<ObjectGraph, Record> accumulator = l.accumulator();
    BinaryOperator<ObjectGraph> combiner = l.combiner();
    Function<ObjectGraph, List<TestUtil.BarEntity>> finisher = l.finisher();

    Record t1 = createRecord(ImmutableMap.of(BAR.ID, 1L, BAR.BAR_, 1), FOO);
    Record t2 = createRecord(ImmutableMap.of(BAR.ID, 2L, BAR.BAR_, 2), FOO);

    ObjectGraph a1 = supplier.get();
    accumulator.accept(a1, t1);
    accumulator.accept(a1, t2);
    List<TestUtil.BarEntity> r1 = finisher.apply(a1);

    ObjectGraph a2 = supplier.get();
    accumulator.accept(a2, t1);
    ObjectGraph a3 = supplier.get();
    accumulator.accept(a3, t2);
    List<TestUtil.BarEntity> r2 = finisher.apply(combiner.apply(a2, a3));

    assertEquals(r1, r2);
  }

  /**
   * From the {@link java.util.stream.Collector} interface:
   *
   * <p>For collectors that do not have the {@code UNORDERED} characteristic, two accumulated
   * results {@code a1} and {@code a2} are equivalent if {@code
   * finisher.apply(a1).equals(finisher.apply(a2))}.
   */
  @Test
  public void testFinisherEquivalence() {
    Entity<TestUtil.FooEntity, ?> foo = new Entity<>(FOO, TestUtil.FooEntity.class);
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);
    Loader<TestUtil.BarEntity> l =
        Loader.of(bar)
            .optionalOneToOne(bar, foo)
            .setZeroOrOneLeft(TestUtil.BarEntity::setFooOptional)
            .build();

    Supplier<ObjectGraph> supplier = l.supplier();
    BiConsumer<ObjectGraph, Record> accumulator = l.accumulator();
    Function<ObjectGraph, List<TestUtil.BarEntity>> finisher = l.finisher();

    ObjectGraph a1 = supplier.get();
    accumulator.accept(a1, createRecord(ImmutableMap.of(BAR.ID, 1L, BAR.BAR_, 2), FOO));

    ObjectGraph a2 = supplier.get();
    accumulator.accept(a2, createRecord(ImmutableMap.of(BAR.ID, 1L, BAR.BAR_, 2), FOO));

    assertEquals(a1, a2);
    assertEquals(finisher.apply(a1), finisher.apply(a2));
  }

  @Test
  public void testCollectorCharacteristics() {
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);
    Loader<TestUtil.BarEntity> l = Loader.of(bar).build();
    assertIterableEquals(Set.of(), l.characteristics());
  }

  @Test
  public void testCollector() {
    Field<?> v = DSL.field("v", Integer.class);
    Entity<TestUtil.FooEntity, ?> foo =
        new Entity<>(FOO, TestUtil.FooEntity.class).withExtraFields(v);
    Entity<TestUtil.BarEntity, ?> bar = new Entity<>(BAR, TestUtil.BarEntity.class);
    Loader<TestUtil.FooEntity> loader =
        Loader.of(foo)
            .manyToMany(foo, bar, FOOBAR)
            .setManyLeft(TestUtil.FooEntity::setBarList)
            .setManyRight(TestUtil.BarEntity::setFooList)
            .and()
            .optionalOneToOne(bar, bar)
            .setZeroOrOneLeft(TestUtil.BarEntity::setOtherBar)
            .build();

    List<TestUtil.FooEntity> entities =
        Stream.of(
                createRecord(
                    ImmutableMap.<Field<?>, Object>builder()
                        .put(FOO.ID, 1L)
                        .put(FOO.FOO_, 1)
                        .put(v, 42)
                        .put(BAR.ID, 1L)
                        // irrelevant foreign key
                        .put(BAR.FOOID, 1L)
                        .put(BAR.BAR_, 1)
                        // forward reference
                        .put(BAR.OTHERBARID, 2L)
                        .put(FOOBAR.FOOID, 1L)
                        .put(FOOBAR.BARID, 1L)
                        .build()),
                createRecord(
                    ImmutableMap.<Field<?>, Object>builder()
                        .put(FOO.ID, 2L)
                        .put(FOO.FOO_, 2)
                        .put(v, 43)
                        .put(BAR.ID, 4L)
                        .put(BAR.FOOID, 4L)
                        .put(BAR.BAR_, 4)
                        // forward reference
                        .put(FOOBAR.FOOID, 4L)
                        .put(FOOBAR.BARID, 1L)
                        .build()),
                createRecord(
                    ImmutableMap.<Field<?>, Object>builder()
                        .put(FOO.ID, 4L)
                        .put(FOO.FOO_, 4)
                        .put(v, 44)
                        // duplicate bar entity
                        .put(BAR.ID, 1L)
                        .put(BAR.FOOID, 1L)
                        .put(BAR.BAR_, 2)
                        .put(BAR.OTHERBARID, 2L)
                        .put(FOOBAR.FOOID, 2L)
                        .put(FOOBAR.BARID, 1L)
                        .build()),
                createRecord(
                    ImmutableMap.<Field<?>, Object>builder()
                        // unmatched foo entity
                        .put(FOO.ID, 6L)
                        .put(FOO.FOO_, 6)
                        .put(v, 24)
                        // self-referencing bar entity
                        .put(BAR.ID, 3L)
                        .put(BAR.FOOID, 3L)
                        .put(BAR.BAR_, 3)
                        .put(BAR.OTHERBARID, 3L)
                        // duplicate relation
                        .put(FOOBAR.FOOID, 1L)
                        .put(FOOBAR.BARID, 1L)
                        .build()),
                createRecord(
                    ImmutableMap.<Field<?>, Object>builder()
                        // duplicate foo entity
                        .put(FOO.ID, 1L)
                        .put(FOO.FOO_, 1)
                        .put(v, 24)
                        .put(BAR.ID, 2L)
                        .put(BAR.FOOID, 2L)
                        .put(BAR.BAR_, 2)
                        // cyclic reference
                        .put(BAR.OTHERBARID, 1L)
                        .put(FOOBAR.FOOID, 1L)
                        .put(FOOBAR.BARID, 2L)
                        .build()))
            .parallel()
            .collect(Loader.toLinkedObjectsWith(loader));

    TestUtil.FooEntity expectedFoo1 = new TestUtil.FooEntity(1L, 1, null, 42);
    TestUtil.FooEntity expectedFoo2 = new TestUtil.FooEntity(2L, 2, null, 43);
    TestUtil.FooEntity expectedFoo4 = new TestUtil.FooEntity(4L, 4, null, 44);
    TestUtil.FooEntity expectedFoo6 = new TestUtil.FooEntity(6L, 6, null, 24);
    TestUtil.BarEntity expectedBar1 = new TestUtil.BarEntity(1L, 1L, 1, 2L, null);
    TestUtil.BarEntity expectedBar2 = new TestUtil.BarEntity(2L, 2L, 2, 1L, null);
    TestUtil.BarEntity expectedBar3 = new TestUtil.BarEntity(3L, 3L, 3, 3L, null);
    TestUtil.BarEntity expectedBar4 = new TestUtil.BarEntity(4L, 4L, 4, null, null);

    expectedFoo1.setBarList(ImmutableList.of(expectedBar1, expectedBar2));
    expectedFoo2.setBarList(ImmutableList.of(expectedBar1));
    expectedFoo4.setBarList(ImmutableList.of(expectedBar1));
    expectedFoo6.setBarList(ImmutableList.of());

    expectedBar1.setFooList(ImmutableList.of(expectedFoo1, expectedFoo4, expectedFoo2));
    expectedBar1.setOtherBar(Optional.of(expectedBar2));

    expectedBar2.setFooList(ImmutableList.of(expectedFoo1));
    expectedBar2.setOtherBar(Optional.of(expectedBar1));

    expectedBar3.setFooList(ImmutableList.of());
    expectedBar3.setOtherBar(Optional.of(expectedBar3));

    expectedBar4.setFooList(ImmutableList.of());
    expectedBar4.setOtherBar(Optional.empty());

    assertIterableEquals(
        ImmutableList.of(expectedFoo1, expectedFoo2, expectedFoo4, expectedFoo6), entities);
  }

  private static Set<IdPair> customRelationLoader(Record record) {
    Long barId = record.get(BAR.ID);
    if (barId == null) {
      return Set.of();
    }
    Long[] fooIds = record.get(FOO.RELATEDFOOIDS);
    if (fooIds == null) {
      return Set.of();
    }
    return Stream.of(fooIds).map(fooId -> IdPair.of(fooId, barId)).collect(Collectors.toSet());
  }
}
