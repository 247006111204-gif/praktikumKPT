"""Uji otomatis: race condition, atomisitas, idempotensi, retry, pembatalan, streaming, crash dan restart.
Jalankan:  python uji_proyek.py      (server dijalankan otomatis oleh skrip ini pada port 50061)
"""
import os
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import grpc

from klien import KlienBioskop

PORT = "50061"
ALAMAT = f"localhost:{PORT}"
HASIL = []


def jalankan_server(db, **env):
    p = subprocess.Popen([sys.executable, "server.py"], env={**os.environ, "PORT": PORT, "DB": db, **env},
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(50):                       # tunggu sampai port siap
        try:
            grpc.channel_ready_future(grpc.insecure_channel(ALAMAT)).result(timeout=0.2)
            return p
        except grpc.FutureTimeoutError:
            pass
    raise RuntimeError("server tidak siap")


def cek(nama, kondisi, detail=""):
    HASIL.append(bool(kondisi))
    print(f"[{'LULUS' if kondisi else 'GAGAL'}] {nama}" + (f" -> {detail}" if detail else ""), flush=True)


def serentak(n, fungsi):
    """Jalankan fungsi(i) pada n thread yang dilepas bersamaan lewat barrier."""
    gerbang = threading.Barrier(n)

    def bungkus(i):
        gerbang.wait()
        return fungsi(i)

    with ThreadPoolExecutor(max_workers=n) as pool:
        return list(pool.map(bungkus, range(n)))


def uji_utama(tmp):
    srv = jalankan_server(os.path.join(tmp, "utama.db"))
    try:
        k = KlienBioskop(ALAMAT)

        # U1: 50 client berebut satu kursi
        hasil = serentak(50, lambda i: KlienBioskop(ALAMAT).pesan("J1", f"user{i}", ["A1"]))
        sukses = sum(r.sukses for r in hasil)
        cek("U1 50 client berebut kursi A1: tepat 1 sukses", sukses == 1 and list(k.denah("J1").terpesan) == ["A1"],
            f"sukses={sukses}, gagal={50 - sukses}")

        # U2: 20 kursi, masing-masing diperebutkan 10 client (200 permintaan)
        kursi = [f"B{n}" for n in range(1, 11)] + [f"C{n}" for n in range(1, 11)]
        hasil = serentak(200, lambda i: KlienBioskop(ALAMAT).pesan("J1", f"u{i}", [kursi[i % 20]]))
        sukses = sum(r.sukses for r in hasil)
        terpesan = set(k.denah("J1").terpesan)
        cek("U2 200 permintaan pada 20 kursi: tepat 20 sukses, tanpa kursi ganda",
            sukses == 20 and terpesan == set(kursi) | {"A1"}, f"sukses={sukses}, kursi terpesan={len(terpesan)}")

        # U3: pemesanan multi-kursi bersifat atomik
        a = k.pesan("J1", "Eka", ["D1", "D2"])
        b = k.pesan("J1", "Fani", ["D2", "D3"])
        cek("U3 multi-kursi atomik (D2 bentrok, D3 tidak ikut terpesan)",
            a.sukses and not b.sukses and "D3" not in k.denah("J1").terpesan, f"kursi_gagal={list(b.kursi_gagal)}")

        # U4: idempotensi, request_id sama dikirim 10 kali bersamaan
        rid = str(uuid.uuid4())
        hasil = serentak(10, lambda i: KlienBioskop(ALAMAT).pesan("J1", "Gita", ["E1"], request_id=rid))
        ids = {r.booking_id for r in hasil}
        cek("U4 request_id sama x10 serentak: satu booking", len(ids) == 1 and all(r.sukses for r in hasil)
            and list(k.denah("J1").terpesan).count("E1") == 1, f"booking_id unik={len(ids)}")

        # U5: pembatalan, kepemilikan, idempotensi, kursi dapat dipesan kembali
        p = k.pesan("J1", "Hadi", ["F1"])
        salah = k.batal(p.booking_id, "Orang Lain")
        rid = str(uuid.uuid4())
        ok1 = k.batal(p.booking_id, "Hadi", request_id=rid)
        ok2 = k.batal(p.booking_id, "Hadi", request_id=rid)        # retry ID sama: balasan lama
        lagi = k.batal(p.booking_id, "Hadi")                        # permintaan baru: sudah tidak ada
        ulang = k.pesan("J1", "Indra", ["F1"])
        cek("U5 batal: pemilik saja, idempoten, kursi bebas lagi",
            (not salah.sukses) and ok1.sukses and ok2.sukses and not lagi.sukses and ulang.sukses,
            f"salah={salah.pesan!r}; ulang={lagi.pesan!r}")

        # U6: streaming mengirim perubahan secara real-time
        diterima, siap = [], threading.Event()

        def pantau():
            s = k.pantau("J2")
            for e in s:
                diterima.append((e.kursi, e.status, e.snapshot, time.time() * 1000 - e.waktu_ms))
                if len(diterima) == 2:
                    s.cancel()
                    break

        t = threading.Thread(target=pantau); t.start(); time.sleep(0.5)
        r = k.pesan("J2", "Joko", ["A1"]); k.batal(r.booking_id, "Joko"); t.join(timeout=5)
        urut = [(x[0], x[1]) for x in diterima]
        cek("U6 PantauKursi menerima TERPESAN lalu DIBATALKAN secara berurutan",
            urut == [("A1", "TERPESAN"), ("A1", "DIBATALKAN")],
            f"{urut}; selisih waktu server-ke-client {[round(x[3], 1) for x in diterima]} ms")
    finally:
        srv.terminate(); srv.wait()


def uji_retry_balasan_terlambat(tmp):
    srv = jalankan_server(os.path.join(tmp, "lambat.db"), LAMBAT="1", LAMBAT_DETIK="3")
    try:
        k = KlienBioskop(ALAMAT, timeout=2.0, log=lambda m: print("      ", m))
        t0 = time.time()
        r = k.pesan("J1", "Kiki", ["A5"])
        n = list(k.denah("J1").terpesan).count("A5")
        cek("U7 balasan terlambat 3 s (> timeout 2 s): retry berhasil, hanya satu booking",
            r.sukses and n == 1 and k.jumlah_retry >= 1, f"retry={k.jumlah_retry}, kursi A5 muncul {n}x, total {time.time() - t0:.1f} s")
    finally:
        srv.terminate(); srv.wait()


def uji_crash_restart(tmp):
    db = os.path.join(tmp, "crash.db")
    srv = jalankan_server(db, CRASH_SETELAH_COMMIT="1")
    rid = str(uuid.uuid4())
    k = KlienBioskop(ALAMAT, timeout=2.0, maks_percobaan=1)
    try:
        k.pesan("J1", "Lina", ["G1", "G2"], request_id=rid)
        gagal = None
    except grpc.RpcError as e:
        gagal = e.code().name
    srv.wait(timeout=5)
    srv = jalankan_server(db)                                     # restart dengan database yang sama
    try:
        k2 = KlienBioskop(ALAMAT)
        r = k2.pesan("J1", "Lina", ["G1", "G2"], request_id=rid)  # retry dengan ID yang sama
        terpesan = list(k2.denah("J1").terpesan)
        lain = k2.pesan("J1", "Mira", ["G1"])                     # kursi harus tetap milik Lina
        cek("U8 crash setelah COMMIT lalu restart: retry tidak membuat booking ganda, state bertahan",
            gagal == "UNAVAILABLE" and r.sukses and terpesan == ["G1", "G2"] and not lain.sukses,
            f"client semula {gagal}; booking_id={r.booking_id}; denah={terpesan}; pemesan lain ditolak={not lain.sukses}")
    finally:
        srv.terminate(); srv.wait()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmp:
        uji_utama(tmp)
        uji_retry_balasan_terlambat(tmp)
        uji_crash_restart(tmp)
    print(f"\nRingkasan: {sum(HASIL)} dari {len(HASIL)} uji lulus")
    sys.exit(0 if all(HASIL) else 1)
