from datetime import datetime, timezone
from typing import List

import base32_lib as base32
from pymongo.database import Database as MongoDatabase


def generate_id(length=10, split_every=4, checksum=True) -> str:
    """Generate random base32 string: a user-shareable ID for a database entity.

    Uses Douglas Crockford Base32 encoding: <https://www.crockford.com/base32.html>

    Default is 8 characters (5-bits each) plus 2 digit characters for ISO 7064 checksum,
    so 2**40 ~ 1 trillion possible values, *much* larger than the number of statements
    feasibly storable by the database. Hyphen splits are optional for human readability,
    and the default is one split after 5 characters, so an example output using the default
    settings is '3sbk2-5j060'.

    :param length: non-hyphen identifier length *including* checksum
    :param split_every: hyphenates every that many characters
    :param checksum: computes and appends ISO-7064 checksum
    :returns: identifier as a string
    """
    return base32.generate(length=length, split_every=split_every, checksum=checksum)


def decode_id(encoded: str, checksum=True) -> int:
    """Decodes generated string ID (via `generate_id`) to a number.

    The string is normalized -- lowercased, hyphens removed,
    {I,i,l,L}=>1 and {O,o}=>0 (user typos corrected) -- before decoding.

    If `checksum` is enabled, raises a ValueError on checksum error.

    :param encoded: string to decode
    :param checksum: extract checksum and validate
    :returns: original number.
    """
    return base32.decode(encoded=encoded, checksum=checksum)


def encode_id(number: int, split_every=4, min_length=10, checksum=True) -> int:
    """Encodes `number` to URI-friendly Douglas Crockford base32 string.

    :param number: number to encode
    :param split_every: if provided, insert '-' every `split_every` characters
                        going from left to right
    :param min_length: 0-pad beginning of string to obtain minimum desired length
    :param checksum: append modulo 97-10 (ISO 7064) checksum to string
    :returns: A random Douglas Crockford base32 encoded string composed only
              of valid URI characters.
    """
    return base32.encode(
        number, split_every=split_every, min_length=min_length, checksum=checksum
    )


# sping: "semi-opaque string" (https://n2t.net/e/n2t_apidoc.html).
#
# Note: The result is always the following list of tuples:
#       ```
#       [
#         ( 2,             512),
#         ( 4,          524288),
#         ( 6,       536870912),
#         ( 8,    549755813888),
#         (10, 562949953421312)
#       ]
#       ````
SPING_SIZE_THRESHOLDS = [(n, (2 ** (5 * n)) // 2) for n in [2, 4, 6, 8, 10]]


def collection_name(naa, shoulder):
    r"""
    Returns a string designed to be used as a MongoDB collection name.

    TODO: Document the function parameters, including expanding the "naa" acronym.
    """
    return f"ids_{naa}_{shoulder}"


def generate_ids(
    mdb: MongoDatabase,
    owner: str,
    populator: str,
    number: int,
    ns: str = "",
    naa: str = "nmdc",
    shoulder: str = "fk4",
) -> List[str]:
    r"""
    Generate the specified number of identifiers, storing them in a MongoDB collection
    whose name is derived from the specified Name-Assigning Authority (NAA) and Shoulder.

    :param mdb: Handle to a MongoDB database
    :param owner: String that will go in the "__ao" field of the identifier record.
                  Callers will oftentimes set this to the name of a Runtime "site"
                  (as in, a "site client" site, not a "Dagster" site).
    :param populator: String that will go in the "who" field of the identifier record.
                      Indicates "who generated this ID." Callers will oftentimes set
                      this to the name of a Runtime "site" (as in, a "site client" site,
                      not a "Dagster" site).
    :param ns: Namespace (see Minter docs); e.g. "changesheets"
    :param naa: Name-Assigning Authority (see Minter docs); e.g. "nmdc"
    :param shoulder: String that will go in the "how" field (see Minter docs); e.g. "sys0"

    This function was written the way it was in an attempt to mirror the ARK spec:
    https://www.ietf.org/archive/id/draft-kunze-ark-41.html (found via: https://arks.org/specs/)

    Deviations from the ARK spec include:
    1. The inclusion of a typecode.
       The inclusion of a typecode came out of discussions with team members,
       who wanted identifiers to include some non-opaque substring that could be used
       to determine what type of resource a given identifier refers to.
    2. Making hyphens mandatory.
       We decided to make the hyphens mandatory, whereas the spec says they are optional.
       > "Hyphens are considered to be insignificant and are always ignored in ARKs."
       > Reference: https://www.ietf.org/archive/id/draft-kunze-ark-41.html#name-character-repertoires
       In our case, we require that users include an identifier's hyphens whenever
       they are using that identifier.
    """
    collection = mdb.get_collection(collection_name(naa, shoulder))
    estimated_document_count = collection.estimated_document_count()
    n_chars = next(
        (
            n
            for n, t in SPING_SIZE_THRESHOLDS
            if (number + estimated_document_count) < t
        ),
        12,
    )
    collected = []

    while True:
        eids = set()
        n_to_generate = number - len(collected)
        while len(eids) < n_to_generate:
            eids.add(generate_id(length=(n_chars + 2), split_every=0, checksum=True))
        eids = list(eids)
        deids = [decode_id(eid) for eid in eids]
        taken = {d["_id"] for d in collection.find({"_id": {"$in": deids}}, {"_id": 1})}
        not_taken = [
            (eid, eid_decoded)
            for eid, eid_decoded in zip(eids, deids)
            if eid_decoded not in taken
        ]
        if not_taken:
            # All attribute names beginning with "__a" are reserved...
            # https://github.com/jkunze/n2t-eggnog/blob/0f0f4c490e6dece507dba710d3557e29b8f6627e/egg#L1882
            # The author of this function opted to refrain from using property names beginning with "_.e",
            # because he thought it would complicate MongoDB queries involving those properties, given that
            # the "." is used as a field delimiter in MongoDB syntax (e.g. "foo.bar.baz").
            docs = [
                {
                    "@context": "https://n2t.net/e/n2t_apidoc.html#identifier-metadata",
                    "_id": eid_decoded,
                    "who": populator,
                    "what": (f"{ns}/{eid}" if ns else "(:tba) Work in progress"),
                    "when": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    "how": shoulder,
                    "where": f"{naa}:{shoulder}{eid}",
                    "__as": "reserved",  # status, public|reserved|unavailable
                    "__ao": owner,  # owner
                    "__ac": datetime.now(timezone.utc).isoformat(
                        timespec="seconds"
                    ),  # created
                }
                for eid, eid_decoded in not_taken
            ]
            collection.insert_many(docs)
            collected.extend(docs)
        if len(collected) == number:
            break
    return [d["where"] for d in collected]


def generate_one_id(
    mdb: MongoDatabase,
    ns: str = "",
    shoulder: str = "sys0",  # "sys0" represents the Runtime
) -> str:
    """Generate unique Crockford Base32-encoded ID for mdb repository.

    Can associate ID with namespace ns to facilitate ID deletion/recycling.

    """
    return generate_ids(
        mdb,
        owner="_system",  # "_system" represents the Runtime
        populator="_system",  # "_system" represents the Runtime
        number=1,
        ns=ns,
        naa="nmdc",
        shoulder=shoulder,
    )[0]


def local_part(id_):
    """nmdc:fk0123 -> fk0123"""
    return id_.split(":", maxsplit=1)[1]
