package tech.picnic.jolo;

import static java.util.Arrays.stream;
import static java.util.stream.Stream.concat;
import static tech.picnic.jolo.Util.equalFieldNames;
import static tech.picnic.jolo.Util.validate;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;

/**
 * Represents a mapping from a jOOQ table to a class. This class is used to store entities that have
 * been loaded from a data set.
 *
 * @param <T> The class that is mapped to.
 */
public final class Entity<T, R extends Record, K> {
  private final Map<K, T> entities = new LinkedHashMap<>();
  private final Table<R> table;
  private final Field<K> primaryKey;
  private final Class<T> type;

  private Field<?>[] fields;
  @Nullable private Field<?>[] resultFields;

  /**
   * Creates a mapping from a jOOQ table to the given class.
   *
   * @param table The table to map. Currently only single-field, long-or-uuid-valued primary keys
   *     are supported.
   * @param type The class that the table for this primary key is mapped to.
   */
  public Entity(Table<R> table, Class<T> type) {
    this(table, type, Util.getPrimaryKey(table));
  }

  /**
   * Creates a mapping from a jOOQ table to the given class, using the given field as primary key.
   * Use this constructor when for instance constructing ad-hoc entities from a select query (as
   * opposed to entities that correspond directly to a table in the database schema).
   *
   * @param table The table to map.
   * @param type The class that the table for this primary key is mapped to.
   * @param primaryKey The field to use as a primary key
   */
  public Entity(Table<R> table, Class<T> type, Field<K> primaryKey) {
    this(table, type, primaryKey, table.fields());
  }

  /** Copy constructor (for internal use). */
  private Entity(Table<R> table, Class<T> type, Field<K> primaryKey, Field<?>[] fields) {
    this.table = table;
    this.primaryKey = primaryKey;
    this.type = type;
    this.fields = fields;
  }

  /**
   * Add extra fields outside the table specified in the constructor that will be passed to the
   * constructor or factory method for each row. This can be used to add, e.g., computed fields.
   *
   * <p>Example: an OrderLine table that does not store the amount of picked items for the order
   * line. The corresponding class has one constructor without, and one with an argument {@code int
   * picked}. If it is mapped from a record that contains a (computed) {@code picked} column, then
   * the constructor with the extra argument is used to bring it into Java land.
   */
  public Entity<T, R, K> withExtraFields(Field<?>... extraFields) {
    this.fields = concat(stream(this.fields), stream(extraFields)).toArray(Field<?>[]::new);
    return this;
  }

  /** Copies this entity, discarding any state. This method is used in a prototype pattern. */
  public Entity<T, R, K> copy() {
    return new Entity<>(table, type, primaryKey, fields);
  }

  /** The table that is mapped by this entity. */
  public Table<R> getTable() {
    return table;
  }

  /** The primary key that this Entity uses to distinguish records. */
  public Field<K> getPrimaryKey() {
    return primaryKey;
  }

  /**
   * Attempts to load an object of type {@code T} from the given record. If the {@link
   * #getPrimaryKey() primary key} is not present in the record, no object is loaded. If the primary
   * key is found in this record, and an object was already loaded for the same key, then the
   * first-loaded object for this key is used.
   */
  public void load(Record record) {
    if (resultFields == null) {
      /*
       * jOOQ does not give us a heads up when it loads the first record, so we manually
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
          equalFieldNames(primaryKey, record.field(primaryKey)),
          "Primary key column %s not found in result record",
          primaryKey);
      resultFields =
          stream(fields).filter(f -> equalFieldNames(f, record.field(f))).toArray(Field<?>[]::new);
    }
    K id = record.get(primaryKey);
    if (id != null) {
      /*
       * The .into(resultFields) makes sure we don't let jOOQ magically use values from fields
       * not included in `this.fields`. E.g., if `this.fields = [FOO.ID, FOO.X]` and the
       * record contains FOO.ID=1 and BAR.X=1, then without this measure, BAR.X would be used
       * instead of FOO.X.
       */
      entities.computeIfAbsent(id, x -> record.into(resultFields).into(type));
    }
  }

  /**
   * Retrieves the object mapped to the primary key with this value.
   *
   * @throws ValidationException if the id is not known.
   * @param id the id
   */
  @SuppressWarnings("NullAway")
  // XXX: Figure out how to convince NullAway we never return `null` here.
  public T get(K id) {
    T result = entities.get(id);
    validate(result != null, "Unknown id requested from table %s: %s", table, id);
    return result;
  }

  /** Returns all objects loaded by this Entity. */
  public Collection<T> getEntities() {
    return Collections.unmodifiableCollection(entities.values());
  }

  /**
   * Returns all objects loaded by this Entity, as a map from primary key values to the
   * corresponding objects.
   */
  Map<K, T> getEntityMap() {
    return Collections.unmodifiableMap(entities);
  }

  @Override
  public String toString() {
    return String.format("Entity<%s, %s>", type.getSimpleName(), primaryKey);
  }
}
