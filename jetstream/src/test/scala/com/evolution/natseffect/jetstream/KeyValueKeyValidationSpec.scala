package com.evolution.natseffect.jetstream

import com.evolution.natseffect.jetstream.KeyValue.InvalidKeyError
import weaver.FunSuite

/** Pins the pure key validation to what jnats enforces: the KV key grammar `put` runs internally plus the subject grammar the wire
  * requires, so a jnats upgrade that changes either fails loudly here instead of silently breaking callers that validate before writing.
  */
object KeyValueKeyValidationSpec extends FunSuite {

  private def accepted(result: Either[InvalidKeyError, Unit]) =
    expect(result.isRight)

  private def rejected(key: String, result: Either[InvalidKeyError, Unit]) =
    result match {
      case Left(InvalidKeyError(k, reason)) => expect.same(key, k) && expect(reason.nonEmpty)
      case Right(())                        => failure(s"expected rejection for [$key]")
    }

  test("write keys with allowed characters are valid") {
    accepted(KeyValue.validateKey("foo")) &&
    accepted(KeyValue.validateKey("foo.bar.baz")) &&
    accepted(KeyValue.validateKey("Key-1_2=3/4.z"))
  }

  test("keys passing put's precheck but invalid on the wire are rejected") {
    rejected("foo.", KeyValue.validateKey("foo.")) &&
    rejected("a..b", KeyValue.validateKey("a..b"))
  }

  test("invalid write keys are rejected with the key and jnats reason") {
    rejected(".foo", KeyValue.validateKey(".foo")) &&
    rejected("fo o", KeyValue.validateKey("fo o")) &&
    rejected("fo*o", KeyValue.validateKey("fo*o")) &&
    rejected("fo>o", KeyValue.validateKey("fo>o")) &&
    rejected("", KeyValue.validateKey("")) &&
    rejected(null, KeyValue.validateKey(null))
  }

  test("wildcard variant accepts * and >") {
    accepted(KeyValue.validateKeyWildcardAllowed("foo.*")) &&
    accepted(KeyValue.validateKeyWildcardAllowed("foo.>")) &&
    accepted(KeyValue.validateKeyWildcardAllowed("*")) &&
    accepted(KeyValue.validateKeyWildcardAllowed(">"))
  }

  test("wildcard variant still rejects malformed keys") {
    rejected(".foo", KeyValue.validateKeyWildcardAllowed(".foo")) &&
    rejected("fo o", KeyValue.validateKeyWildcardAllowed("fo o")) &&
    rejected("foo.", KeyValue.validateKeyWildcardAllowed("foo.")) &&
    rejected("a..b", KeyValue.validateKeyWildcardAllowed("a..b")) &&
    rejected("", KeyValue.validateKeyWildcardAllowed("")) &&
    rejected(null, KeyValue.validateKeyWildcardAllowed(null))
  }

  test("wildcard variant rejects misplaced wildcards") {
    rejected("fo*o", KeyValue.validateKeyWildcardAllowed("fo*o")) &&
    rejected("foo.>.bar", KeyValue.validateKeyWildcardAllowed("foo.>.bar"))
  }

  test("error message carries the key") {
    KeyValue.validateKey("fo o") match {
      case Left(error) => expect(error.getMessage.contains("fo o"))
      case Right(())   => failure("expected rejection")
    }
  }
}
