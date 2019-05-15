package tech.picnic.jolo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static tech.picnic.jolo.TestUtil.createRecord;
import static tech.picnic.jolo.data.schema.Tables.FOO;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record2;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import tech.picnic.jolo.TestUtil.FooEntity;
import tech.picnic.jolo.data.schema.tables.Foo;
import tech.picnic.jolo.data.schema.tables.records.FooRecord;

// XXX: Suppressing NullAway warnings pending resolution of
// https://github.com/jOOQ/jOOQ/issues/4748.
@SuppressWarnings("NullAway")
public final class EntityTest {
  @Test
  public void testGetters() {
    Entity<?, ?> aEntity = new Entity<>(FOO, FooEntity.class);
    assertEquals(aEntity.getPrimaryKey(), FOO.ID);
    assertEquals(aEntity.getTable(), FOO);
  }

  @Test
  public void testGettersAliased() {
    Table<?> bar = FOO.as("BAR");
    Entity<?, ?> aEntity = new Entity<>(bar, FooEntity.class);
    assertEquals(aEntity.getPrimaryKey(), bar.field(FOO.ID));
    assertEquals(aEntity.getTable(), bar);
  }

  @Test
  public void testLoad() {
    Entity<?, ?> aEntity = new Entity<>(FOO, FooEntity.class);
    aEntity.load(new FooRecord(1L, 1, null));
    Assertions.assertEquals(aEntity.get(1), new FooEntity(1L, 1, null));
  }

  @Test
  public void testLoadExtraAttributes() {
    Field<?> v = DSL.field("v", Integer.class);
    Entity<?, ?> aEntity = new Entity<>(FOO, FooEntity.class).withExtraFields(v);
    Record record =
        createRecord(
            ImmutableMap.of(FOO.ID, 1L, FOO.FOO_, 1, FOO.RELATEDFOOIDS, new Long[0], v, 2));
    aEntity.load(record);
    Assertions.assertEquals(aEntity.get(1), new FooEntity(1L, 1, new Long[0], 2));
  }

  @Test
  public void testLoadAdHocTable() {
    Field<Long> id = DSL.field("id", Long.class);
    Field<Integer> foo = DSL.field("foo", Integer.class);
    Table<Record2<Long, Integer>> adHoc = DSL.select(id, foo).asTable("AdHoc");
    Entity<?, ?> aEntity = new Entity<>(adHoc, FooEntity.class, adHoc.field(id));
    Record record = createRecord(ImmutableMap.of(adHoc.field(id), 1L, adHoc.field(foo), 1));
    aEntity.load(record);
    Assertions.assertEquals(aEntity.get(1), new FooEntity(1L, 1, null));
  }

  @Test
  public void testCopy() {
    Entity<?, ?> aEntity = new Entity<>(FOO, FooEntity.class);
    aEntity.load(new FooRecord(1L, 1, null));
    assertTrue(aEntity.copy().getEntities().isEmpty());
  }

  @Test
  public void testLoadMultiple() {
    Entity<?, ?> aEntity = new Entity<>(FOO, FooEntity.class);
    aEntity.load(new FooRecord(2L, 1, null));
    aEntity.load(new FooRecord(1L, 1, null));
    assertIterableEquals(
        aEntity.getEntities(),
        ImmutableList.of(new FooEntity(2L, 1, null), new FooEntity(1L, 1, null)));
  }

  @Test
  public void testLoadTwice() {
    Entity<?, ?> aEntity = new Entity<>(FOO, FooEntity.class);
    aEntity.load(new FooRecord(1L, 1, null));
    aEntity.load(new FooRecord(1L, 2, null));
    assertEquals(aEntity.getEntityMap(), ImmutableMap.of(1L, new FooEntity(1L, 1, null)));
  }

  @Test
  public void testLoadAbsent() {
    Entity<?, ?> aEntity = new Entity<>(FOO, FooEntity.class);
    aEntity.load(new FooRecord(null, null, null));
    assertTrue(aEntity.getEntities().isEmpty());
  }

  @Test
  public void testLoadWrongTable() {
    Foo bar = FOO.as("BAR");
    Entity<?, ?> aEntity = new Entity<>(FOO, FooEntity.class);
    Record record = createRecord(ImmutableMap.of(bar.ID, 1L, bar.FOO_, 1));
    assertThrows(ValidationException.class, () -> aEntity.load(record));
  }
}
