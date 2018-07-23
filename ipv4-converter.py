import ipaddress
import itertools

myIP = ipaddress.ip_address("fe80::20be:cdff:fe1f:6c2d")
hextets = myIP.exploded.split(":")

# from itertools recipes
def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)

new_groups = [int(a+b, base=16) for (a, b) in grouper(hextets, 2)]

ipv4ish = '.'.join(map(str, new_groups))
