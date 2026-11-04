import datetime
import sys
import time
import xmlrpc.client

URL = "http://localhost:8000/"


def l11():
    proxy = xmlrpc.client.ServerProxy(URL)
    print("pangkat(2, 10) =", proxy.pangkat(2, 10))
    print("akar(144)      =", proxy.akar(144))
    try:
        proxy.akar(-4)
    except xmlrpc.client.Fault as e:
        print("akar(-4) -> Fault:", e.faultString)


def l12():
    proxy = xmlrpc.client.ServerProxy(URL)
    data = [4, 8, 15, 16, 23, 42]
    hasil = proxy.statistik(data)
    print("statistik", data, "=", hasil)
    print("tipe hasil:", type(hasil).__name__)
    print("--- XML permintaan ---")
    print(xmlrpc.client.dumps((data,), "statistik"))
    print("--- XML balasan ---")
    print(xmlrpc.client.dumps((hasil,), methodresponse=True))


def l13():
    proxy = xmlrpc.client.ServerProxy(URL)
    sekarang = datetime.datetime(2026, 11, 4, 9, 30, 15)
    balik = proxy.echo(sekarang)
    print("(a) datetime dikirim :", repr(sekarang))
    print("    datetime kembali :", repr(balik), "| tipe:", type(balik).__name__)
    proxy2 = xmlrpc.client.ServerProxy(URL, use_datetime=True)
    print("    use_datetime=True:", repr(proxy2.echo(sekarang)))
    print("(b) bilangan besar")
    for n in (2**31 - 1, 2**31, 2**63):
        try:
            print(f"    echo({n}) ->", proxy.echo(n))
        except OverflowError as e:
            print(f"    echo({n}) -> OverflowError: {e}")
    besar = 2**100
    balik = int(proxy.echo(str(besar)))
    print("(c) dikirim sebagai string, 2**100 kembali sama persis:", balik == besar)
    f = proxy.echo(float(10**30 + 1))
    print("    float(10**30+1) kembali:", f, "| sama dengan aslinya?", int(f) == 10**30 + 1)


def l14():
    proxy = xmlrpc.client.ServerProxy(URL)
    proxy.tambah(1, 1)  # pemanasan koneksi

    def lokal(a, b):
        return a + b

    t0 = time.perf_counter()
    for i in range(100):
        proxy.tambah(i, i)
    rpc = (time.perf_counter() - t0) * 1000
    t0 = time.perf_counter()
    for i in range(100):
        lokal(i, i)
    lok = (time.perf_counter() - t0) * 1000
    print(f"100 panggilan RPC   : {rpc:.2f} ms ({rpc / 100:.3f} ms/panggilan)")
    print(f"100 panggilan lokal : {lok:.4f} ms")
    print(f"RPC / lokal         : {rpc / lok:.0f}x")


def l15():
    proxy = xmlrpc.client.ServerProxy(URL)
    for i in range(1, 11):
        try:
            print(f"panggilan {i}: tambah({i}, 1) = {proxy.tambah(i, 1)}", flush=True)
        except ConnectionRefusedError as e:
            print(f"panggilan {i}: server tidak dapat dihubungi ({type(e).__name__}: {e})", flush=True)
        except (ConnectionResetError, ConnectionAbortedError) as e:
            print(f"panggilan {i}: koneksi terputus ({type(e).__name__})", flush=True)
        except xmlrpc.client.ProtocolError as e:
            print(f"panggilan {i}: respons HTTP {e.errcode}", flush=True)
        except OSError as e:
            print(f"panggilan {i}: kesalahan jaringan ({type(e).__name__}: {e})", flush=True)
        time.sleep(1)


if __name__ == "__main__":
    globals()[sys.argv[1]]()
