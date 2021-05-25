from toolz import keyfilter


def omit(blacklist, d):
    return keyfilter(lambda k: k not in blacklist, d)


def pick(whitelist, d):
    return keyfilter(lambda k: k in whitelist, d)
