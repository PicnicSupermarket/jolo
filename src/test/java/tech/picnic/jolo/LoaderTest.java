package tech.picnic.jolo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static tech.picnic.jolo.TestUtil.createRecord;
import static tech.picnic.jolo.data.schema.Tables.BAR;
import static tech.picnic.jolo.data.schema.Tables.BAZ;
import static tech.picnic.jolo.data.schema.Tables.FOO;
import static tech.picnic.jolo.data.schema.Tables.FOOBAR;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.jooq.Field;
import org.jooq.Record;
import org.junit.jupiter.api.Test;
import tech.picnic.jolo.TestUtil.BarEntity;
import tech.picnic.jolo.TestUtil.BazEntity;
import tech.picnic.jolo.TestUtil.FooEntity;
import tech.picnic.jolo.data.schema.tables.Bar;
import tech.picnic.jolo.data.schema.tables.Foo;

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
    Entity<FooEntity, ?, Long> foo = new Entity<>(fooTable, FooEntity.class);
    Entity<BarEntity, ?, Long> bar = new Entity<>(barTable, BarEntity.class);
    Loader<FooEntity> l =
        LoaderFactory.create(foo)
            .oneToMany(foo, bar)
            .setManyLeft(FooEntity::setBarList)
            .setOneRight(BarEntity::setFoo)
            .build()
            .newLoader();
    l.next(
        createRecord(
            ImmutableMap.of(
                fooTable.ID, 1L,
                fooTable.FOO_, 1,
                barTable.ID, 1L,
                barTable.FOOID, 1L,
                barTable.BAR_, 2)));
    l.next(
        createRecord(
            ImmutableMap.of(
                fooTable.ID, 1L,
                fooTable.FOO_, 1,
                barTable.ID, 2L,
                barTable.FOOID, 1L,
                barTable.BAR_, 3)));

    FooEntity expectedFoo = new FooEntity(1L, 1, null);
    BarEntity expectedBar1 = new BarEntity(1L, 1L, 2, null, null);
    BarEntity expectedBar2 = new BarEntity(2L, 1L, 3, null, null);
    expectedBar1.setFoo(expectedFoo);
    expectedBar2.setFoo(expectedFoo);
    expectedFoo.setBarList(ImmutableList.of(expectedBar1, expectedBar2));

    assertEquals(expectedFoo, l.getOne());
  }

  @Test
  public void testOneToManyWrongWayAround() {
    Entity<FooEntity, ?, Long> foo = new Entity<>(FOO, FooEntity.class);
    Entity<BarEntity, ?, Long> bar = new Entity<>(BAR, BarEntity.class);
    assertThrows(
        IllegalArgumentException.class, () -> LoaderFactory.create(foo).oneToMany(bar, foo));
  }

  @Test
  public void testOneToOne() {
    Entity<FooEntity, ?, Long> foo = new Entity<>(FOO, FooEntity.class);
    Entity<BarEntity, ?, Long> bar = new Entity<>(BAR, BarEntity.class);
    Loader<BarEntity> l =
        LoaderFactory.create(bar)
            .oneToOne(bar, foo)
            .setOneLeft(BarEntity::setFoo)
            .build()
            .newLoader();
    // Add the same record twice to check that there are no duplicates
    l.next(
        createRecord(
            ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1, BAR.ID, 1L, BAR.FOOID, 1L, BAR.BAR_, 2)));
    l.next(
        createRecord(
            ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1, BAR.ID, 1L, BAR.FOOID, 1L, BAR.BAR_, 2)));

    FooEntity expectedFoo = new FooEntity(1L, 1, null);
    BarEntity expectedBar = new BarEntity(1L, 1L, 2, null, null);
    expectedBar.setFoo(expectedFoo);

    assertEquals(expectedBar, l.getOne());
  }

  @Test
  public void testOneToOneAmbiguous() {
    Entity<BarEntity, ?, Long> bar = new Entity<>(BAR, BarEntity.class);
    Entity<BazEntity, ?, Long> baz = new Entity<>(BAZ, BazEntity.class);
    assertThrows(ValidationException.class, () -> LoaderFactory.create(bar).oneToOne(bar, baz));
  }

  @Test
  public void testOneToZeroOrOne() {
    Entity<FooEntity, ?, Long> foo = new Entity<>(FOO, FooEntity.class);
    Entity<BarEntity, ?, Long> bar = new Entity<>(BAR, BarEntity.class);
    Loader<FooEntity> l =
        LoaderFactory.create(foo)
            .oneToZeroOrOne(foo, bar)
            .setZeroOrOneLeft(FooEntity::setBarOptional)
            .setOneRight(BarEntity::setFoo)
            .build()
            .newLoader();

    l.next(
        createRecord(
            ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1, BAR.ID, 1L, BAR.FOOID, 1L, BAR.BAR_, 2)));
    l.next(createRecord(ImmutableMap.of(FOO.ID, 2L, FOO.FOO_, 2), BAR));

    FooEntity expectedFoo1 = new FooEntity(1L, 1, null);
    FooEntity expectedFoo2 = new FooEntity(2L, 2, null);
    BarEntity expectedBar = new BarEntity(1L, 1L, 2, null, null);
    expectedBar.setFoo(expectedFoo1);
    expectedFoo1.setBarOptional(Optional.of(expectedBar));
    expectedFoo2.setBarOptional(Optional.empty());

    assertEquals(ImmutableList.of(expectedFoo1, expectedFoo2), l.getList());
  }

  @Test
  public void testOptionalOneToOne() {
    Entity<FooEntity, ?, Long> foo = new Entity<>(FOO, FooEntity.class);
    Entity<BarEntity, ?, Long> bar = new Entity<>(BAR, BarEntity.class);
    Loader<BarEntity> l =
        LoaderFactory.create(bar)
            .optionalOneToOne(bar, foo)
            .setZeroOrOneLeft(BarEntity::setFooOptional)
            .build()
            .newLoader();
    // Add the same record twice to check that there are no duplicates
    l.next(
        createRecord(
            ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1, BAR.ID, 1L, BAR.FOOID, 1L, BAR.BAR_, 2)));
    l.next(
        createRecord(
            ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1, BAR.ID, 1L, BAR.FOOID, 1L, BAR.BAR_, 2)));

    FooEntity expectedFoo = new FooEntity(1L, 1, null);
    BarEntity expectedBar = new BarEntity(1L, 1L, 2, null, null);
    expectedBar.setFooOptional(Optional.of(expectedFoo));

    assertEquals(expectedBar, l.getOne());
  }

  @Test
  public void testEmptyOptionalOneToOne() {
    Entity<FooEntity, ?, Long> foo = new Entity<>(FOO, FooEntity.class);
    Entity<BarEntity, ?, Long> bar = new Entity<>(BAR, BarEntity.class);
    Loader<BarEntity> l =
        LoaderFactory.create(bar)
            .optionalOneToOne(bar, foo)
            .setZeroOrOneLeft(BarEntity::setFooOptional)
            .build()
            .newLoader();
    // Add the same record twice to check that there are no duplicates
    l.next(createRecord(ImmutableMap.of(BAR.ID, 1L, BAR.BAR_, 2), FOO, BAR));
    l.next(createRecord(ImmutableMap.of(BAR.ID, 1L, BAR.BAR_, 2), FOO, BAR));

    BarEntity expectedBar = new BarEntity(1L, null, 2, null, null);
    expectedBar.setFooOptional(Optional.empty());

    assertEquals(expectedBar, l.getOne());
  }

  @Test
  public void testZeroOrOneToOne() {
    Entity<FooEntity, ?, Long> foo = new Entity<>(FOO, FooEntity.class);
    Entity<BarEntity, ?, Long> bar = new Entity<>(BAR, BarEntity.class);
    Loader<BarEntity> l =
        LoaderFactory.create(bar)
            .optionalOneToOne(bar, foo)
            .setZeroOrOneLeft(BarEntity::setFooOptional)
            .build()
            .newLoader();
    l.next(createRecord(ImmutableMap.of(BAR.ID, 1L, BAR.BAR_, 2), FOO));

    BarEntity expectedBar = new BarEntity(1L, null, 2, null, null);
    expectedBar.setFooOptional(Optional.empty());

    assertEquals(expectedBar, l.getOne());
  }

  @Test
  public void testZeroOrOneToMany() {
    Entity<FooEntity, ?, Long> foo = new Entity<>(FOO, FooEntity.class);
    Entity<BarEntity, ?, Long> bar = new Entity<>(BAR, BarEntity.class);
    Loader<BarEntity> l =
        LoaderFactory.create(bar)
            .zeroOrOneToMany(foo, bar)
            .setManyLeft(FooEntity::setBarList)
            .setZeroOrOneRight(BarEntity::setFooOptional)
            .build()
            .newLoader();
    l.next(
        createRecord(
            ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1, BAR.ID, 1L, BAR.FOOID, 1L, BAR.BAR_, 2)));
    l.next(createRecord(ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1, BAR.ID, 2L, BAR.BAR_, 3)));

    FooEntity expectedFoo = new FooEntity(1L, 1, null);
    BarEntity expectedBar1 = new BarEntity(1L, 1L, 2, null, null);
    BarEntity expectedBar2 = new BarEntity(2L, null, 3, null, null);
    expectedBar1.setFooOptional(Optional.of(expectedFoo));
    expectedBar2.setFooOptional(Optional.empty());
    expectedFoo.setBarList(ImmutableList.of(expectedBar1));

    assertEquals(ImmutableSet.of(expectedBar1, expectedBar2), l.getSet());
  }

  @Test
  public void testZeroOrOneToManyRecursiveReference() {
    Entity<BarEntity, ?, Long> bar = new Entity<>(BAR, BarEntity.class);
    Loader<BarEntity> l =
        LoaderFactory.create(bar)
            .zeroOrOneToMany(bar, bar)
            .setZeroOrOneRight(BarEntity::setOtherBar)
            .build()
            .newLoader();
    l.next(createRecord(ImmutableMap.of(BAR.ID, 1L, BAR.BAR_, 1, BAR.OTHERBARID, 2L)));
    l.next(createRecord(ImmutableMap.of(BAR.ID, 2L, BAR.BAR_, 2, BAR.OTHERBARID, 2L)));

    BarEntity expectedBar1 = new BarEntity(1L, null, 1, 2L, null);
    BarEntity expectedBar2 = new BarEntity(2L, null, 2, 2L, null);
    expectedBar1.setOtherBar(Optional.of(expectedBar2));
    expectedBar2.setOtherBar(Optional.of(expectedBar2));

    assertEquals(ImmutableList.of(expectedBar1, expectedBar2), l.getList());
  }

  @Test
  public void testNToOneThrowsIfManyAreFound() {
    Entity<FooEntity, ?, Long> foo = new Entity<>(FOO, FooEntity.class);
    Entity<BarEntity, ?, Long> bar = new Entity<>(BAR, BarEntity.class);
    Loader<BarEntity> l =
        LoaderFactory.create(bar)
            .oneToOne(bar, foo)
            .setOneLeft(BarEntity::setFoo)
            .build()
            .newLoader();
    l.next(
        createRecord(
            ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1, BAR.ID, 1L, BAR.FOOID, 1L, BAR.BAR_, 2)));
    l.next(
        createRecord(
            ImmutableMap.of(FOO.ID, 2L, FOO.FOO_, 2, BAR.ID, 1L, BAR.FOOID, 2L, BAR.BAR_, 2)));

    FooEntity expectedFoo = new FooEntity(1L, 1, null);
    BarEntity expectedBar = new BarEntity(1L, 1L, 2, null, null);
    expectedBar.setFoo(expectedFoo);

    assertThrows(ValidationException.class, l::getOne);
  }

  @Test
  public void testManyToMany() {
    Entity<FooEntity, ?, Long> foo = new Entity<>(FOO, FooEntity.class);
    Entity<BarEntity, ?, Long> bar = new Entity<>(BAR, BarEntity.class);
    Loader<FooEntity> l =
        LoaderFactory.create(foo)
            .manyToMany(foo, bar, FOOBAR)
            .setManyLeft(FooEntity::setBarList)
            .setManyRight(BarEntity::setFooList)
            .build()
            .newLoader();
    l.next(
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

    FooEntity expectedFoo = new FooEntity(1L, 1, null);
    BarEntity expectedBar = new BarEntity(1L, 1L, 2, null, null);
    expectedBar.setFooList(ImmutableList.of(expectedFoo));
    expectedFoo.setBarList(ImmutableList.of(expectedBar));

    assertEquals(expectedFoo, l.getOne());
  }

  @Test
  public void testGetOptional() {
    Entity<FooEntity, ?, ?> foo = new Entity<>(FOO, FooEntity.class);
    Loader<FooEntity> l = LoaderFactory.create(foo).build().newLoader();

    // Initially, it should return empty
    assertEquals(l.getOptional(), Optional.empty());

    // After one record, we should be able to retrieve the entity
    l.next(createRecord(ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1)));
    assertEquals(l.getOptional(), Optional.of(new FooEntity(1L, 1, null)));

    // IllegalArgumentException if we added more than one record
    l.next(createRecord(ImmutableMap.of(FOO.ID, 2L, FOO.FOO_, 1)));
    assertThrows(IllegalArgumentException.class, l::getOptional);
  }

  @Test
  public void testRelationWithDummyRelationLoader() {
    Entity<FooEntity, ?, Long> foo = new Entity<>(FOO, FooEntity.class);
    Entity<BarEntity, ?, Long> bar = new Entity<>(BAR, BarEntity.class);

    Loader<FooEntity> l =
        LoaderFactory.create(foo)
            .manyToMany(foo, bar, FOOBAR)
            .setManyLeft(FooEntity::setBarList)
            .setManyRight(BarEntity::setFooList)
            .setRelationLoader((record, pairs) -> pairs.add(Relation.Pair.of(1L, 1L)))
            .build()
            .newLoader();

    l.next(
        createRecord(
            ImmutableMap.<Field<?>, Object>builder()
                .put(FOO.ID, 1L)
                .put(FOO.FOO_, 1)
                .put(BAR.ID, 1L)
                .put(BAR.FOOID, 1L)
                .put(BAR.BAR_, 2)
                .build()));

    assertEquals(l.getOne().getBarList().get(0).getId(), 1L);
  }

  @Test
  public void testRelationWithCustomRelationLoader() {
    Entity<FooEntity, ?, Long> foo = new Entity<>(FOO, FooEntity.class);
    Entity<BarEntity, ?, Long> bar = new Entity<>(BAR, BarEntity.class);

    Loader<FooEntity> l =
        LoaderFactory.create(foo)
            .manyToMany(foo, bar, FOOBAR)
            .setManyLeft(FooEntity::setBarList)
            .setManyRight(BarEntity::setFooList)
            .setRelationLoader(LoaderTest::customRelationLoader)
            .build()
            .newLoader();

    l.next(
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
    l.next(
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

    for (FooEntity entity : l.get()) {
      assertEquals(entity.getBarList().get(0).getId(), 1L);
    }
  }

  @Test
  public void testFallbackToForeignKeyRelation() {
    Entity<FooEntity, ?, Long> foo = new Entity<>(FOO, FooEntity.class);
    Entity<BarEntity, ?, Long> bar = new Entity<>(BAR, BarEntity.class);

    Loader<FooEntity> l =
        LoaderFactory.create(foo)
            .manyToMany(foo, bar, FOOBAR)
            .setManyLeft(FooEntity::setBarList)
            .setManyRight(BarEntity::setFooList)
            .build()
            .newLoader();

    l.next(
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
    l.next(
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

    List<FooEntity> fooEntitiesInLoader = l.getList();
    assertEquals(fooEntitiesInLoader.get(0).getBarList().get(0).getId(), 1L);
    assertEquals(fooEntitiesInLoader.get(1).getBarList(), ImmutableList.of());
  }

  private static void customRelationLoader(Record record, Set<Relation.Pair<Long>> pairs) {
    Long barId = record.get(BAR.ID);
    if (barId == null) {
      return;
    }
    Long[] fooIds = record.get(FOO.RELATEDFOOIDS);
    if (fooIds == null) {
      return;
    }
    Stream.of(fooIds).map(fooId -> Relation.Pair.of(fooId, barId)).forEach(pairs::add);
  }
}
