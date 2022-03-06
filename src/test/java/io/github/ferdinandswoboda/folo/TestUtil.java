package io.github.ferdinandswoboda.folo;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multiset;
import com.google.errorprone.annotations.Var;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.DefaultDSLContext;

final class TestUtil {
  private static final DSLContext MOCK_CONTEXT = new DefaultDSLContext(new DefaultConfiguration());

  private TestUtil() {}

  /**
   * Creates a record with the given fields mapped to the given values. If any of the fields are
   * table fields, and some of the other fields in the same table are not given in the mapping, then
   * null values are inserted automatically for those other fields. If {@code nullTables} is given,
   * then also null values are inserted for all fields in those tables.
   */
  static Record createRecord(ImmutableMap<Field<?>, Object> values, Table<?>... nullTables) {
    Stream<Field<?>> tableFields =
        Stream.concat(
                Stream.of(nullTables),
                values.keySet().stream()
                    .filter(TableField.class::isInstance)
                    .map(f -> (TableField<?, ?>) f)
                    .map(TableField::getTable))
            .flatMap(Table::fieldStream);
    Stream<Field<?>> extraFields = values.keySet().stream().filter(f -> !(f instanceof TableField));
    Record result =
        MOCK_CONTEXT.newRecord(Stream.concat(tableFields, extraFields).toArray(Field<?>[]::new));

    values.forEach((f, v) -> setValue(result, f, v));
    return result;
  }

  private static <T> void setValue(Record record, Field<T> field, Object value) {
    checkArgument(value == null || field.getType().isInstance(value));
    record.set(field, field.getType().cast(value));
  }

  @SuppressWarnings("FieldCanBeFinal")
  public static final class FooEntity {
    private final Multiset<FooEntity> other = HashMultiset.create();
    private final long id;
    private final int foo;
    @Nullable private Object[] relatedFooIds;
    @Nullable private Integer v;
    @Nullable private ImmutableList<BarEntity> barList;
    @Nullable private Optional<BarEntity> barOptional;

    FooEntity(long id, int foo) {
      this(id, foo, null);
    }

    FooEntity(long id, int foo, @Nullable Object[] relatedFooIds) {
      this.id = id;
      this.foo = foo;
      this.relatedFooIds = relatedFooIds;
    }

    FooEntity(long id, int foo, @Nullable Object[] relatedFooIds, int v) {
      this.id = id;
      this.foo = foo;
      this.relatedFooIds = relatedFooIds;
      this.v = v;
    }

    public ImmutableList<BarEntity> getBarList() {
      return requireNonNull(barList, "barList not yet set");
    }

    void setBarList(List<BarEntity> barList) {
      verify(this.barList == null, "barList already set");
      this.barList = ImmutableList.copyOf(barList);
    }

    void setBarOptional(Optional<BarEntity> barOptional) {
      verify(this.barOptional == null, "barOptional already set");
      this.barOptional = barOptional;
    }

    void setRelatedFooIds(Object[] relatedFooIds) {
      verify(this.relatedFooIds == null, "relatedFooIds already set");
      this.relatedFooIds = relatedFooIds;
    }

    /**
     * Because we have dependencies between foo and bar, our equality check becomes a bit more
     * involved. We compute a greatest fixpoint here: if sub-calculations may assume that {@code
     * this == o}, can we conclude that {@code this == o}? In other words: rather than computing
     * straightforward equality, we are computing bisimilarity here.
     *
     * <p>Example:
     *
     * <pre>
     * bar1.foo == foo1, foo1.barList == [bar1] bar2.foo == foo2, foo2.barList == [bar2]
     * </pre>
     *
     * <p>If bar1 and bar2 agree on all attributes except foo (because foo1 != foo2), and foo1 and
     * foo2 agree on all attributes except barList (because [bar1] != [bar2]), then we still
     * consider them equal.
     *
     * <p>This is tricky business, but works in this case because we know exactly the classes that
     * are referring to each other (Foo and Bar), and we are using this trick here to break the
     * recursion. The upshot is that we can test that the references contained in the objects are
     * also "equal", which is what we are interested in for the purpose of testing the loading of
     * relationship graphs.
     */
    @Override
    public boolean equals(@Nullable Object o) {
      if (o == this || other.contains(o)) {
        return true;
      }
      if (!(o instanceof FooEntity)) {
        return false;
      }
      FooEntity fooEntity = (FooEntity) o;
      @Var
      boolean result =
          id == fooEntity.id
              && foo == fooEntity.foo
              && Arrays.equals(relatedFooIds, fooEntity.relatedFooIds)
              && Objects.equals(v, fooEntity.v);
      if (result) {
        other.add(fooEntity);
        result = Objects.equals(barList, fooEntity.barList);
        other.remove(fooEntity);
      }
      return result;
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, foo, Arrays.hashCode(relatedFooIds), v);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("id", id)
          .add("foo", foo)
          .add("relatedFooIds", relatedFooIds)
          .add("v", v)
          .toString();
    }
  }

  public static final class BarEntity {
    private final Multiset<BarEntity> other = HashMultiset.create();
    private final long id;
    @Nullable private final Long fooId;
    private final int bar;
    @Nullable private final Long otherBarId;
    @Nullable private final Long bazId;

    @Nullable private FooEntity foo;
    @Nullable private Optional<FooEntity> fooOptional;
    @Nullable private ImmutableList<FooEntity> fooList;
    @Nullable private Optional<BarEntity> otherBar;

    BarEntity(
        long id, @Nullable Long fooId, int bar, @Nullable Long otherBarId, @Nullable Long bazId) {
      this.id = id;
      this.fooId = fooId;
      this.bar = bar;
      this.otherBarId = otherBarId;
      this.bazId = bazId;
    }

    public long getId() {
      return id;
    }

    void setFoo(FooEntity foo) {
      verify(this.foo == null, "foo already set");
      this.foo = foo;
    }

    void setFooList(List<FooEntity> fooList) {
      verify(this.foo == null, "fooList already set");
      this.fooList = ImmutableList.copyOf(fooList);
    }

    void setFooOptional(Optional<FooEntity> fooOptional) {
      verify(this.fooOptional == null, "fooOptional already set");
      this.fooOptional = fooOptional;
    }

    void setOtherBar(Optional<BarEntity> barOptional) {
      verify(this.otherBar == null, "otherBar already set");
      this.otherBar = barOptional;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (o == this || other.contains(o)) {
        return true;
      }
      if (!(o instanceof BarEntity)) {
        return false;
      }
      BarEntity barEntity = (BarEntity) o;
      @Var
      boolean result =
          id == barEntity.id
              && Objects.equals(fooId, barEntity.fooId)
              && Objects.equals(bazId, barEntity.bazId)
              && bar == barEntity.bar
              && Objects.equals(otherBarId, barEntity.otherBarId);
      if (result) {
        other.add(barEntity);
        result =
            Objects.equals(foo, barEntity.foo)
                && Objects.equals(fooList, barEntity.fooList)
                && Objects.equals(fooOptional, barEntity.fooOptional)
                && Objects.equals(otherBar, barEntity.otherBar);
        other.remove(barEntity);
      }
      return result;
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, fooId, bar, otherBarId);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("id", id)
          .add("fooId", fooId)
          .add("bar", bar)
          .add("bazId", bazId)
          .add("otherBarId", otherBarId)
          .toString();
    }
  }

  static final class BazEntity {
    BazEntity(long id, long barId) {
      // This class is only here to allow us to use the Baz table in our tests.
    }
  }
}
