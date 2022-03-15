package io.github.ferdinandswoboda.folo;

import static io.github.ferdinandswoboda.folo.data.schema.base.Tables.FOO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableMap;
import io.github.ferdinandswoboda.folo.data.schema.base.tables.Foo;
import io.github.ferdinandswoboda.folo.data.schema.base.tables.records.FooRecord;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record2;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public final class EntityTest {
  @Test
  public void testGetters() {
    Entity<TestUtil.FooEntity, FooRecord> aEntity = new Entity<>(FOO, TestUtil.FooEntity.class);
    assertEquals(FOO.ID, aEntity.getPrimaryKey());
    assertEquals(FOO, aEntity.getTable());
  }

  @Test
  public void testGettersAliased() {
    Table<?> bar = FOO.as("BAR");
    Entity<TestUtil.FooEntity, ?> aEntity = new Entity<>(bar, TestUtil.FooEntity.class);
    assertEquals(bar.field(FOO.ID), aEntity.getPrimaryKey());
    assertEquals(bar, aEntity.getTable());
  }

  @Test
  public void testLoad() {
    Entity<TestUtil.FooEntity, FooRecord> aEntity = new Entity<>(FOO, TestUtil.FooEntity.class);
    @SuppressWarnings("NullAway")
    FooRecord fooRecord = new FooRecord(1L, 1, null);
    TestUtil.FooEntity object = aEntity.load(fooRecord);
    Assertions.assertEquals(new TestUtil.FooEntity(1L, 1, null), object);
  }

  @Test
  public void testLoadExtraAttributes() {
    Field<?> v = DSL.field("v", Integer.class);
    Entity<TestUtil.FooEntity, FooRecord> aEntity =
        new Entity<>(FOO, TestUtil.FooEntity.class).withExtraFields(v);
    Record record =
        TestUtil.createRecord(
            ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1, FOO.RELATEDFOOIDS, new Long[0], v, 2));
    TestUtil.FooEntity object = aEntity.load(record);
    Assertions.assertEquals(new TestUtil.FooEntity(1L, 1, new Long[0], 2), object);
  }

  @Test
  public void testLoadAdHocTable() {
    Field<Long> id = DSL.field("id", Long.class);
    Field<Integer> foo = DSL.field("foo", Integer.class);
    Table<Record2<Long, Integer>> adHoc = DSL.select(id, foo).asTable("AdHoc");
    Entity<TestUtil.FooEntity, Record2<Long, Integer>> aEntity =
        new Entity<>(adHoc, TestUtil.FooEntity.class, adHoc.field(id));
    Record record =
        TestUtil.createRecord(ImmutableMap.of(adHoc.field(id), 1L, adHoc.field(foo), 1));
    TestUtil.FooEntity object = aEntity.load(record);
    Assertions.assertEquals(new TestUtil.FooEntity(1L, 1, null), object);
  }

  @Test
  public void testLoadWrongTable() {
    Foo bar = FOO.as("BAR");
    Entity<TestUtil.FooEntity, FooRecord> aEntity = new Entity<>(FOO, TestUtil.FooEntity.class);
    Record record = TestUtil.createRecord(ImmutableMap.of(bar.ID, 1L, bar.FOO_, 1));
    assertThrows(ValidationException.class, () -> aEntity.load(record));
  }
}
