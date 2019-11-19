package tech.picnic.jolo;

/** Interface implemented by classes that can create {@link Loader} objects. */
public interface LoaderFactory<T> {
  /** Creates a new {@link LoaderFactory} using the default implementation. */
  static <T> LoaderFactoryBuilder<T> create(Entity<T, ?, ?> mainEntity) {
    return new LoaderFactoryBuilderImpl<>(mainEntity);
  }

  /** Create a new {@link Loader}. */
  Loader<T> newLoader();
}
