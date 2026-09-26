# ADR-0035: A request id is redeemed only with the secret it joined with

**Status:** Accepted. Amends [ADR-0010](0010-client-supplied-request-id.md).

## 1. Context

ADR-0010 has the client supply its own `request_id`. That id then plays two roles (issue #62):

- **A public value.** `GET /v1/queue_num` carries it in the query string, and the polled cache
  behaviour keys on it (ADR-0013). So it is written to CloudFront and API Gateway access logs,
  browser history, corporate proxies, `Referer` headers and anything a visitor screenshots.
- **The whole of the admission credential.** `POST /v1/generate_token` minted a session cookie for
  whoever presented it.

Anyone who saw a URL could take that visitor's admission. The theft is silent, and it is worth the
most at the front of the queue, which is where a log reader would harvest from.

`/v1/join` is a direct API Gateway → SQS integration (ADR-0005), so the service cannot hand a
server-minted secret back at join time. Any fix has to work with a secret the client makes itself.

## 2. Decision

**The browser generates a 32-byte possession secret alongside the `request_id`. The join carries
only `h = base64url(SHA-256(secret))`, and `generate_token` requires the secret itself.**

- **Client.** `waiting.js` mints the id and the secret together and stores them as one value,
  `<request_id>.<secret>`, across the existing storage chain. Neither can be recovered without the
  other. The cookie tier of that chain is scoped to `/_wr/`, so the secret is never sent to the
  origin. A legacy bare id has no secret behind it, so it is replaced rather than reused.
- **Join.** The API Gateway schema requires `h` (43 base64url characters) and does not accept a
  `secret` field. `assign_position` stores `h` on the `PreQueue` or `Positions` row it writes, and
  drops a join without a well-formed `h` as malformed. Such a row could never be redeemed, so it
  should not consume a position.
- **Admission.** `generate_token` reads the secret from the JSON body only; a query string would
  put it back in access logs. It hashes the secret and compares the result, in constant time
  (`aws-lc-rs`), against the digest on the row that answers the position: `Positions` if one
  exists, else `PreQueue`. It checks this *before* anything about the queue. A caller who knows
  only the id gets `403`, learns no position and leaves no claim. The admission claim copies the
  digest onto the `Positions` row it creates for a pre-queue member, so that member's later calls,
  which read that row first, still have one to verify.
- **Digest form.** SHA-256 over the secret's ASCII (the base64url string), not over the decoded
  bytes, because that is what `crypto.subtle.digest` in the browser hashes without extra
  encoding. A fixed vector in `wr_common::crypto` and a client test against Node's SHA-256 pin
  both sides.

## 3. Consequences

- **Knowing a `request_id` alone no longer admits anyone.** It is still public, still in the
  `/queue_num` cache key, and still reveals that visitor's position through `/queue_num`. Position
  is not a credential.
- **Nothing is added to the burst path.** The client does the hashing, and the join stays a
  direct SQS write. The `generate_token` check is a hash and a compare on a row it already reads.
  Each row gains one 43-byte attribute.
- **A stored id whose secret is lost cannot be redeemed.** The two are stored as one value, so
  that only happens if storage is lost entirely, in which case the visitor had already lost their
  place. A `403` tells the waiting page to stop and say so rather than poll.
- **It needs a secure context.** `crypto.subtle` exists only over https, which CloudFront always
  serves. An http page says so instead of joining.
- **Deploy between events.** Rows written before this change carry no digest and are refused at
  redemption. Deploying mid-event would strand everyone already in line.
- **It does not stop someone who holds the secret.** A visitor who hands over both the id and the
  secret has handed over their place. Binding the resulting session to the client is
  [ADR-0036](0036-optional-session-ip-binding.md) (issue #61).
