import math
from xmlrpc.server import SimpleXMLRPCServer


def tambah(a, b):
    return a + b


def kali(a, b):
    return a * b


def bagi(a, b):
    if b == 0:
        raise ValueError("Pembagian dengan nol")
    return a / b


def pangkat(a, b):
    return a ** b


def akar(x):
    if x < 0:
        raise ValueError("Akar bilangan negatif tidak didefinisikan")
    return math.sqrt(x)


def statistik(daftar):
    return {"min": min(daftar), "max": max(daftar), "rata2": sum(daftar) / len(daftar)}


def echo(x):
    return x


server = SimpleXMLRPCServer(("0.0.0.0", 8000), allow_none=True, logRequests=True)
server.register_introspection_functions()
for f in (tambah, kali, bagi, pangkat, akar, statistik, echo):
    server.register_function(f, f.__name__)
print("Server XML-RPC berjalan di port 8000...", flush=True)
server.serve_forever()
