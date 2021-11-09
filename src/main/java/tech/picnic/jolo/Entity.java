package tech.picnic.jolo;

import static java.util.Arrays.stream;
import static java.util.stream.Stream.concat;
import static tech.picnic.jolo.Util.validate;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;

/**
 * Represents a mapping from a {@link Table table} to a {@link Class class}. This class is used to
 * load and instantiate objects from a data set.
 *
 * @param <T> The class that is mapped to.
 */
public final class Entity<T, R extends Record> {
  private final Table<R> table;
  private final Field<Long> primaryKey;
  private final Class<T> type;

  private final Field<?>[] fields;
  @Nullable private Field<?>[] resultFields;

  /**
   * Creates a mapping from a {@link Table table} to the given {@link Class class}.
   *
   * @param table The table to map. Currently only single-field, long-valued primary keys are
   *     supported.
   * @param type The class that the table for this primary key is mapped to.
   */
  public Entity(Table<R> table, Class<T> type) {
    this(table, type, Util.getPrimaryKey(table));
  }

  /**
   * Creates a mapping from a {@link Table table} to the given {@link Class class}, using the given
   * field as primary key. Use this constructor when for instance constructing ad-hoc entities from
   * a select query (as opposed to entities that correspond directly to a table in the database
   * schema).
   *
   * @param table The table to map.
   * @param type The class that the table for this primary key is mapped to.
   * @param primaryKey The field to use as a primary key
   */
  public Entity(Table<R> table, Class<T> type, Field<Long> primaryKey) {
    this(table, type, primaryKey, table.fields());
  }

  /** Copy constructor (for internal use). */
  private Entity(Table<R> table, Class<T> type, Field<Long> primaryKey, Field<?>[] fields) {
    this.table = table;
    this.primaryKey = primaryKey;
    this.type = type;
    this.fields = fields;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Entity<?, ?> entity = (Entity<?, ?>) o;
    return Objects.equals(table, entity.table)
        && Objects.equals(primaryKey, entity.primaryKey)
        && Objects.equals(type, entity.type)
        && Arrays.equals(fields, entity.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, primaryKey, type, Arrays.hashCode(fields));
  }

  @Override
  public String toString() {
    return String.format("Entity<%s, %s>", type.getSimpleName(), primaryKey);
  }

  /**
   * Add extra fields outside the table specified in the constructor that will be passed to the
   * constructor or factory method for each row. This can be used to add, e.g., computed fields.
   *
   * <p>Example: an OrderLine table that does not store the amount of picked items for the order
   * line. The corresponding class has one constructor without, and one with an argument {@code int
   * picked}. If it is mapped from a record that contains a (computed) {@code picked} column, then
   * the constructor with the extra argument is used to bring it into Java land.
   *
   * @apiNote This method returns a new object.
   */
  public Entity<T, R> withExtraFields(Field<?>... extraFields) {
      Field<?>[] extendedFields = concat(stream(fields), stream(extraFields).filter(Objects::nonNull))
              .toArray(Field<?>[]::new);
      return new Entity<>(table, type, primaryKey, extendedFields);
  }

  /** The table that is mapped by this entity. */
  public Table<R> getTable() {
    return table;
  }

  /** The primary key that this entity uses to distinguish records. */
  public Field<Long> getPrimaryKey() {
    return primaryKey;
  }

  /** The given record's ID, corresponding to this entity. */
  Optional<Long> getId(Record record) {
    check(record);
    return Optional.ofNullable(record.get(primaryKey));
  }

  /** Loads an object of type {@code T} from the given record. */
  T load(Record record) {
    check(record);
    /*
     * The .into(resultFields) makes sure we don't let jOOQ magically use values from fields
     * not included in `this.fields`. E.g., if `this.fields = [FOO.ID, FOO.X]` and the
     * record contains FOO.ID=1 and BAR.X=1, then without this measure, BAR.X would be used
     * instead of FOO.X.
     */
    T result = record.into(resultFields).into(type);
    Objects.requireNonNull(result);
    return result;
  }

  /**
   * Check that the given record contains all expected fields and sets them.
   *
   * @implNote For performance reasons, this check is only performed for the first loaded record and
   *     is a no-op otherwise.
   */
  private void check(Record record) {
    if (resultFields == null) {
      /*
       * jOOQ does not give us a heads-up when it loads the first record, so we manually
       * detect when the first record is loaded and perform some extra checks in that case.
       * Most importantly, we check that the primary key is actually present, but we also
       * figure out which of the expected fields are actually present in the record. If we do
       * not take this precaution, the default behaviour of jOOQ is to load values from
       * similarly named fields, which can lead to very unexpected results.
       *
       * Since this method is called for every record, we only do all of this once. This is
       * not ideal because we have no control over re-use of this object, but in a normal
       * usage scenario this object will only ever be used with the same query, and therefore
       * we will always retrieve records with the same set of columns.
       */
      validate(
          primaryKey.equals(record.field(primaryKey)),
          "Primary key column %s not found in result record",
          primaryKey);
      resultFields = stream(fields).filter(f -> f.equals(record.field(f))).toArray(Field<?>[]::new);
    }
  }
}
