package tech.picnic.jolo;

import org.immutables.value.Value;

@Value.Immutable(builder = false)
@Value.Style(
    allParameters = true,
    typeAbstract = "*Interface",
    typeImmutable = "*",
    visibility = Value.Style.ImplementationVisibility.PUBLIC)
interface PairInterface {
  long getLeftId();

  long getRightId();
}
