import base32_lib as base32
import pymongo
from pydantic import constr


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


def generate_id_unique(
    mdb: pymongo.database.Database = None, ns: str = None, **generate_id_kwargs
) -> str:
    """Generate unique Crockford Base32-encoded ID for mdb repository.

    Can associate ID with namespace ns to facilitate ID deletion/recycling.

    """
    get_one = True
    collection = mdb.get_collection("ids")
    while get_one:
        eid = generate_id(**generate_id_kwargs)
        eid_decoded = decode_id(eid)
        get_one = collection.count_documents({"_id": eid_decoded}) > 0
    collection.insert_one({"_id": eid_decoded, "ns": ns})
    return eid


# NO i, l, o or u. Optional '-'s.
Base32Id = constr(regex=r"^[0-9abcdefghjkmnpqrstvwxyz\-]+$")
IdShoulder = constr(regex=r"^[abcdefghjkmnpqrstvwxyz\-]+[0-9]$")
