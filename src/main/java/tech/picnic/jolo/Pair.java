package tech.picnic.jolo;

import java.util.Objects;

public final class Pair {
  private final long leftId;
  private final long rightId;

  Pair(long leftId, long rightId) {
    this.leftId = leftId;
    this.rightId = rightId;
  }

  public static Pair of(long leftId, long rightId) {
    return new Pair(leftId, rightId);
  }

  long getLeftId() {
    return leftId;
  }

  long getRightId() {
    return rightId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Pair pair = (Pair) o;
    return leftId == pair.leftId && rightId == pair.rightId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(leftId, rightId);
  }
}
