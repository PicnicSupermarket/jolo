package tech.picnic.jolo;

import org.immutables.value.Value;

/** A pair of related IDs. */
@Value.Immutable(builder = false)
@Value.Style(
    allParameters = true,
    typeAbstract = "*Interface",
    typeImmutable = "*",
    visibility = Value.Style.ImplementationVisibility.PUBLIC)
interface IdPairInterface {
  /** The left-hand ID of the relation. */
  long getLeftId();

  /** The right-hand ID of the relation. */
  long getRightId();
}
