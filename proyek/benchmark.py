"""Pengujian kinerja: latensi rata-rata dan throughput untuk 1, 5, dan 20 client bersamaan.
Server harus sudah berjalan (python server.py). Argumen opsional: alamat server, jumlah permintaan per skenario.
"""
import json
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from klien import KlienBioskop

ALAMAT = sys.argv[1] if len(sys.argv) > 1 else "localhost:50051"
TOTAL = int(sys.argv[2]) if len(sys.argv) > 2 else 600
SEMUA_KURSI = [f"{chr(65 + b)}{n}" for b in range(26) for n in range(1, 101)]   # 2600 kursi pada jadwal UJI
pakai = 0


def ukur(n_client, kerja):
    """Bagi TOTAL permintaan ke n_client thread; setiap thread memakai KlienBioskop (channel) sendiri."""
    lat, lock = [], threading.Lock()
    per_client = TOTAL // n_client
    gerbang = threading.Barrier(n_client + 1)

    def thread_client(c):
        k = KlienBioskop(ALAMAT)
        k.denah("J1")                                  # pemanasan koneksi
        gerbang.wait()
        lokal = []
        for i in range(per_client):
            t = time.perf_counter()
            kerja(k, c, i)
            lokal.append((time.perf_counter() - t) * 1000)
        with lock:
            lat.extend(lokal)
        k.tutup()

    with ThreadPoolExecutor(max_workers=n_client) as pool:
        futs = [pool.submit(thread_client, c) for c in range(n_client)]
        gerbang.wait()
        t0 = time.perf_counter()
        for f in futs:
            f.result()
        total = time.perf_counter() - t0
    n = per_client * n_client
    return {"client": n_client, "permintaan": n, "waktu_s": round(total, 2), "throughput": round(n / total, 1),
            "latensi_ms": round(statistics.mean(lat), 2), "p95_ms": round(sorted(lat)[int(0.95 * len(lat)) - 1], 2)}


def pesan(k, c, i):
    r = k.pesan("UJI", f"u{c}", [SEMUA_KURSI[pesan.mulai + c * pesan.per + i]])   # blok kursi unik per client
    assert r.sukses


if __name__ == "__main__":
    for jenis in ("PesanKursi", "GetDenah"):
        for n in (1, 5, 20):
            if jenis == "PesanKursi":
                pesan.mulai, pesan.per = pakai, TOTAL // n      # setiap skenario memakai blok kursi baru (tanpa bentrok)
                hasil = ukur(n, pesan)
                pakai += TOTAL
            else:
                hasil = ukur(n, lambda k, c, i: k.denah("J1"))
            hasil["rpc"] = jenis
            print(json.dumps(hasil), flush=True)
