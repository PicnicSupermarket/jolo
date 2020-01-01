package tech.picnic.jolo;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jooq.Record;
import org.jooq.RecordHandler;

/**
 * Record handler that loads entity-relation graphs from a data set. To create a {@code Loader}, use
 * {@link LoaderFactory#create}. The loader can be used as follows:
 *
 * <pre>{@code
 * // In static initialisation code, set up the loader factory
 * private static final Entity<T> MY_ENTITY = ...;
 * private static final LoaderFactory<T> FACTORY =
 *     LoaderFactory.create(MY_ENTITY)
 *             .relation(...)
 *             .oneToMany(...)
 *             .setOneLeft(...)
 *             .and()
 *             .relation(...)
 *             ...;
 *
 * // At runtime:
 * Query query = ...;
 * Collection<T> result = query.fetchInto(FACTORY.newLoader()).get();
 * }</pre>
 *
 * <p>It is highly recommended to initialise the loader factory as early as possible, because during
 * initialisation a number of validations are performed. Initialising at application start-up
 * therefore makes it possible to detect any misconfiguration before the query is first executed.
 */
public final class Loader<T> implements RecordHandler<Record> {
  private final Entity<T, ?> mainEntity;
  private final Set<Entity<?, ?>> entities;
  private final List<Relation<?, ?>> relations;
  private boolean linked = false;

  Loader(Entity<T, ?> mainEntity, Set<Entity<?, ?>> entities, List<Relation<?, ?>> relations) {
    this.mainEntity = mainEntity;
    this.entities = entities;
    this.relations = relations;
  }

  @Override
  public void next(Record record) {
    entities.forEach(e -> e.load(record));
    relations.forEach(r -> r.load(record));
  }

  /** Returns all objects loaded by this loader. */
  public Collection<T> get() {
    link();
    return mainEntity.getEntities();
  }

  /** Returns all objects loaded by this loader. */
  public Stream<T> stream() {
    return get().stream();
  }

  /** Returns all objects loaded by this loader. */
  public <R, A> R collect(Collector<? super T, A, R> collector) {
    return stream().collect(collector);
  }

  /** Returns all objects loaded by this loader. */
  public List<T> getList() {
    return stream().collect(Collectors.toList());
  }

  /**
   * Returns all objects loaded by this loader. Note that the entity type must implement {@link
   * Object#equals} and {@link Object#hashCode} appropriately in order for this method to return a
   * correct result.
   */
  public Set<T> getSet() {
    return stream().collect(Collectors.toSet());
  }

  /**
   * Returns the single object loaded by this loader, or throws an exception.
   *
   * @throws IllegalArgumentException if more than one object was loaded.
   * @throws java.util.NoSuchElementException if the loader is empty.
   */
  public T getOne() {
    return getOptional().orElseThrow(NoSuchElementException::new);
  }

  /**
   * Returns the single object loaded by this loader, if any.
   *
   * @throws IllegalArgumentException if more than one object was loaded.
   */
  public Optional<T> getOptional() {
    Iterator<T> it = get().iterator();
    if (it.hasNext()) {
      T result = it.next();
      if (it.hasNext()) {
        throw new IllegalArgumentException("More than one entity was loaded");
      }
      return Optional.of(result);
    }
    return Optional.empty();
  }

  private void link() {
    if (!linked) {
      relations.forEach(Relation::link);
      linked = true;
    }
  }
}
