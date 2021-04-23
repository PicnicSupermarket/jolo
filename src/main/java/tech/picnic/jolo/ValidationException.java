package tech.picnic.jolo;

public final class ValidationException extends RuntimeException {
  /** The {@link java.io.Serializable serialization} ID. */
  private static final long serialVersionUID = 1L;

  ValidationException(String message) {
    super(message);
  }
}
