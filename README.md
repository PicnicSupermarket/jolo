# Jolo

[![Build Status][travisci-badge]][travisci-builds]
[![Maven Central][maven-central-badge]][maven-central-browse]
[![SonarCloud Quality Gate][sonarcloud-badge-quality-gate]][sonarcloud-dashboard]
[![SonarCloud Bugs][sonarcloud-badge-bugs]][sonarcloud-measure-reliability]
[![SonarCloud Vulnerabilities][sonarcloud-badge-vulnerabilities]][sonarcloud-measure-security]
[![SonarCloud Maintainability][sonarcloud-badge-maintainability]][sonarcloud-measure-maintainability]
[![BCH compliance][bettercodehub-badge]][bettercodehub-results]

Short for _jOOQ Loader_. A utility library to add basic object-relation mapping to your [jOOQ][jooq] code.

![Picnic-jolo][jolo-image]

## How to Install

Artifacts are hosted on [Maven's Central Repository][maven-central-browse]:

### Gradle

```groovy
dependencies {
  compile 'tech.picnic.jolo:jolo:0.0.3'
}
```

### Maven

```xml

<dependency>
  <groupId>tech.picnic.jolo</groupId>
  <artifactId>jolo</artifactId>
  <version>0.0.3</version>
</dependency>
```

## Features

- Easy specification of relations between entities using a chaining API.
- Implements `java.util.stream.Collector` allowing object instantiation using jOOQ's native "collect" method; the loader
  can additionally call setters to instantiate relationships between entities.
- Performs foreign key checks to see whether the defined relationships make sense.
- Extra checks on field names of returned records to prevent loading fields from one table as fields of another (no
  implicit conversion of `FOO.FIELD` to `BAR.FIELD`).
- Supports circular references.
- Supports adding extra (non-table) fields to entities.

## Limitations

- Only primary / foreign keys of (Java) type `long` are supported. We have no intention to support composite foreign
  keys for the time being. For keys of different types (e.g.  `String`) we would accept pull requests, but only if this
  does not further complicate the interface of the library (no long type parameter lists).
- Relation mapping does not work yet for entities that are not based on a table in the DB schema.

## Example usage

Let's assume we are working with the following table structure:

```sql
CREATE TABLE Dog (
  id bigserial PRIMARY KEY,
  name text,
  weight int
);

CREATE TABLE Flea (
  id bigserial PRIMARY KEY,
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
decrease. (There are exceptions to this rule, e.g. when more code is deleted than added.)

[bettercodehub-badge]: https://bettercodehub.com/edge/badge/PicnicSupermarket/jolo?branch=master
[bettercodehub-results]: https://bettercodehub.com/results/PicnicSupermarket/jolo
[jolo-image]: https://i.imgur.com/MThi0ae.jpg
[jooq]: https://www.jooq.org
[maven-central-badge]: https://img.shields.io/maven-central/v/tech.picnic.jolo/jolo.svg
[maven-central-browse]: https://repo1.maven.org/maven2/tech/picnic/jolo/
[new-issue]: https://github.com/PicnicSupermarket/jolo/issues/new
[new-pr]: https://github.com/PicnicSupermarket/jolo/compare
[sonarcloud-badge-bugs]: https://sonarcloud.io/api/project_badges/measure?project=tech.picnic.jolo%3Ajolo&metric=bugs
[sonarcloud-badge-maintainability]: https://sonarcloud.io/api/project_badges/measure?project=tech.picnic.jolo%3Ajolo&metric=sqale_rating
[sonarcloud-badge-quality-gate]: https://sonarcloud.io/api/project_badges/measure?project=tech.picnic.jolo%3Ajolo&metric=alert_status
[sonarcloud-badge-vulnerabilities]: https://sonarcloud.io/api/project_badges/measure?project=tech.picnic.jolo%3Ajolo&metric=vulnerabilities
[sonarcloud-dashboard]: https://sonarcloud.io/dashboard?id=tech.picnic.jolo%3Ajolo
[sonarcloud-measure-reliability]: https://sonarcloud.io/component_measures?id=tech.picnic.jolo%3Ajolo&metric=Reliability
[sonarcloud-measure-security]: https://sonarcloud.io/component_measures?id=tech.picnic.jolo%3Ajolo&metric=Security
[sonarcloud-measure-maintainability]: https://sonarcloud.io/component_measures?id=tech.picnic.jolo%3Ajolo&metric=Maintainability
[travisci-badge]: https://travis-ci.org/PicnicSupermarket/jolo.svg?branch=master
[travisci-builds]: https://travis-ci.org/PicnicSupermarket/jolo
