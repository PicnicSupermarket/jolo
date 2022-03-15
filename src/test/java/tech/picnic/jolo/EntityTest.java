package tech.picnic.jolo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static tech.picnic.jolo.TestUtil.createRecord;
import static tech.picnic.jolo.data.schema.base.Tables.FOO;

import com.google.common.collect.ImmutableMap;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record2;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Test;
import tech.picnic.jolo.TestUtil.FooEntity;
import tech.picnic.jolo.data.schema.base.tables.Foo;
import tech.picnic.jolo.data.schema.base.tables.records.FooRecord;

public final class EntityTest {
  @Test
  public void testGetters() {
    Entity<FooEntity, FooRecord> aEntity = new Entity<>(FOO, FooEntity.class);
    assertEquals(FOO.ID, aEntity.getPrimaryKey());
    assertEquals(FOO, aEntity.getTable());
  }

  @Test
  public void testGettersAliased() {
    Table<?> bar = FOO.as("BAR");
    Entity<FooEntity, ?> aEntity = new Entity<>(bar, FooEntity.class);
    assertEquals(bar.field(FOO.ID), aEntity.getPrimaryKey());
    assertEquals(bar, aEntity.getTable());
  }

  @Test
  public void testLoad() {
    Entity<FooEntity, FooRecord> aEntity = new Entity<>(FOO, FooEntity.class);
    @SuppressWarnings("NullAway")
    FooRecord fooRecord = new FooRecord(1L, 1, null);
    FooEntity object = aEntity.load(fooRecord);
    assertEquals(new FooEntity(1L, 1, null), object);
  }

  @Test
  public void testLoadExtraAttributes() {
    Field<?> v = DSL.field("v", Integer.class);
    Entity<FooEntity, FooRecord> aEntity = new Entity<>(FOO, FooEntity.class).withExtraFields(v);
    Record record =
        createRecord(
            ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1, FOO.RELATEDFOOIDS, new Long[0], v, 2));
    FooEntity object = aEntity.load(record);
    assertEquals(new FooEntity(1L, 1, new Long[0], 2), object);
  }

  @Test
  public void testLoadAdHocTable() {
    Field<Long> id = DSL.field("id", Long.class);
    Field<Integer> foo = DSL.field("foo", Integer.class);
    Table<Record2<Long, Integer>> adHoc = DSL.select(id, foo).asTable("AdHoc");
    Entity<FooEntity, Record2<Long, Integer>> aEntity =
        new Entity<>(adHoc, FooEntity.class, adHoc.field(id));
    Record record = createRecord(ImmutableMap.of(adHoc.field(id), 1L, adHoc.field(foo), 1));
    FooEntity object = aEntity.load(record);
    assertEquals(new FooEntity(1L, 1, null), object);
  }

  @Test
  public void testLoadWrongTable() {
    Foo bar = FOO.as("BAR");
    Entity<FooEntity, FooRecord> aEntity = new Entity<>(FOO, FooEntity.class);
    Record record = createRecord(ImmutableMap.of(bar.ID, 1L, bar.FOO_, 1));
    assertThrows(ValidationException.class, () -> aEntity.load(record));
  }
}
