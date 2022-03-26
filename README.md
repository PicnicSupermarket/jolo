# Folo

[![Build Status][gh-actions-badge]][gh-actions-builds]
[![Maven Central][maven-central-badge]][maven-central-browse]
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Successor to [Jolo], a _jOOQ Loader_. Folo is a utility library to add basic object-relation mapping to
your [jOOQ][jooq] code.

## How to Install

Artifacts are hosted on [Maven's Central Repository][maven-central-browse]:

### Gradle

```groovy
dependencies {
    compile 'io.github.ferdinand-swoboda.folo:folo:1.0.0'
}
```

### Maven

```xml

<dependency>
    <groupId>io.github.ferdinand-swoboda.folo</groupId>
    <artifactId>folo</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Features

- Easy specification of relations between entities using a chaining API.
- Implements `java.util.stream.Collector` allowing object instantiation using jOOQ's native `ResultQuery#collect`
  method; the loader can additionally call setters to instantiate relationships between entities.
- Performs foreign key checks to see whether the defined relationships make sense.
- Extra checks on field names of returned records to prevent loading fields from one table as fields of another (no
  implicit conversion of `FOO.FIELD` to `BAR.FIELD`).
- Supports circular references.
- Supports adding extra (non-table) fields to entities.

## Limitations/Potential improvements

- Make use of Java modules (introduced in Java 9).
- Investigate `@Nullable`, `@NonNull`, `@CheckReturnValue` and other annotations.
- Only primary / foreign keys of (Java) type `long` are supported i.e. no composite or UUID keys.
- Relation mapping does not work for entities that are not based on a table in the DB schema.

## Example usage

Let's assume we are working with the following table structure:

```sql
CREATE TABLE Dog
(
    id     bigserial PRIMARY KEY,
    name   text,
    weight int
);

CREATE TABLE Flea
(
    id     bigserial PRIMARY KEY,
    dog_id bigint REFERENCES Dog,
    weight int
)
```

And in Java you have modelled your dogs and fleas using POJOs that are serialisable using standard jOOQ functionality:

```java
class Dog {
    private long id;
    private String name;
    private int weight;
    private List<Flea> fleas;

    /* Getters and setters for ID, name & weight. */

    @Transient
    public List<Flea> getFleas() {
        return fleas;
    }

    public void setFleas(List<Flea> fleas) {
        this.fleas = fleas;
    }
}

class Flea {
    private long id;
    private int weight;

    /* Getters and setters. */
}
```

Using this library, you can specify how to instantiate the relationship between those POJOs
(i.e., how to fill the `fleas` property of `Dog`):

```java
class LoaderUtil {
    static Loader<Dog> createLoader() {
        var dog = new Entity<>(Tables.DOG, Dog.class);
        var flea = new Entity<>(Tables.FLEA, Flea.class);
        return Loader.of(dog).oneToMany(dog, flea).setManyLeft(Dog::setFleas).build();
    }
}
```

Then in the code that executes the query, you can use the loader to instantiate and link POJO classes:

```java
class Repository {
    private static final Loader<Dog> LOADER = createLoader();

    private final DSLContext context;

    void dogLog() {
        List<Dog> dogs = context.select()
                .from(DOG)
                .leftJoin(FLEA)
                .on(FLEA.DOG_ID.eq(DOG.ID))
                .collect(toLinkedObjectsWith(LOADER));

        for (Dog dog : dogs) {
            int fleaWeight = dog.getFleas().stream().mapToInt(Flea::getWeight).sum();
            LOG.info("%s is %.0f%% fleas",
                    dog.getName(),
                    fleaWeight * 100.0 / dog.getWeight());
        }
    }
}
```

## Contributing

Contributions are welcome! Feel free to file an [issue][new-issue] or open a
[pull request][new-pr].

When submitting changes, please make every effort to follow existing conventions and style in order to keep the code as
readable as possible. New code must be covered by tests. As a rule of thumb, overall test coverage should not
decrease, performance metrics should stay the same or improve.

[jolo]: https://github.com/picnicsupermarket/jolo

[jooq]: https://www.jooq.org

[maven-central-badge]: https://img.shields.io/maven-central/v/io.github.ferdinand-swoboda.folo/folo
[maven-central-browse]: https://search.maven.org/artifact/io.github.ferdinand-swoboda.folo/folo

[new-issue]: https://github.com/ferdinand-swoboda/folo/issues/new

[new-pr]: https://github.com/ferdinand-swoboda/folo/compare

[gh-actions-badge]: https://github.com/ferdinand-swoboda/folo/actions/workflows/development.yaml/badge.svg
[gh-actions-builds]: https://github.com/ferdinand-swoboda/folo/actions/workflows/development.yaml
