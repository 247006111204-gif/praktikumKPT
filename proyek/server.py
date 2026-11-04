"""Server reservasi bioskop (gRPC).

Concurrency : satu LOCK global melindungi pemeriksaan kursi, penulisan SQLite, dan perubahan state memori.
Kegagalan   : request_id disimpan di tabel idem dalam transaksi yang sama dengan booking (at-most-once,
              tahan restart). State dipulihkan dari SQLite saat server hidup kembali.
"""
import json
import os
import queue
import random
import re
import sqlite3
import threading
import time
import uuid
from concurrent import futures

import grpc

import bioskop_pb2 as pb
import bioskop_pb2_grpc as pb_grpc

PORT = os.environ.get("PORT", "50051")
DB = os.environ.get("DB", "bioskop.db")
WORKERS = int(os.environ.get("WORKERS", "32"))
LAMBAT = float(os.environ.get("LAMBAT", "0"))                   # peluang balasan terlambat (uji toleransi kegagalan)
LAMBAT_DETIK = float(os.environ.get("LAMBAT_DETIK", "3"))
CRASH = os.environ.get("CRASH_SETELAH_COMMIT") == "1"          # matikan proses setelah COMMIT, sebelum balasan

JADWAL = {j.id: j for j in [
    pb.Jadwal(id="J1", film="Petualangan Senja", studio="Studio 1", waktu="19:00", baris=8, kolom=10),
    pb.Jadwal(id="J2", film="Misteri Kota Tua", studio="Studio 2", waktu="21:00", baris=6, kolom=8),
    pb.Jadwal(id="UJI", film="Studio Uji Beban", studio="Studio X", waktu="00:00", baris=26, kolom=100),
]}
POLA_KURSI = re.compile(r"^([A-Z])([1-9][0-9]*)$")

LOCK = threading.Lock()
TERPESAN = {j: {} for j in JADWAL}   # jadwal_id -> {kursi: booking_id}
BOOKING = {}                         # booking_id -> {"jadwal_id", "pemesan", "kursi": [...]}
IDEM = {}                            # "pesan:<id>" / "batal:<id>" -> byte balasan
SUBS = {j: [] for j in JADWAL}       # jadwal_id -> antrean per pemantau
SUBS_LOCK = threading.Lock()
conn = None


def init_db():
    global conn
    conn = sqlite3.connect(DB, check_same_thread=False, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("CREATE TABLE IF NOT EXISTS booking(booking_id TEXT PRIMARY KEY, jadwal_id TEXT, pemesan TEXT, kursi TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS idem(kunci TEXT PRIMARY KEY, respons BLOB)")
    for bid, jid, pemesan, kursi in conn.execute("SELECT booking_id, jadwal_id, pemesan, kursi FROM booking"):
        daftar = json.loads(kursi)
        BOOKING[bid] = {"jadwal_id": jid, "pemesan": pemesan, "kursi": daftar}
        for k in daftar:
            TERPESAN[jid][k] = bid
    for kunci, respons in conn.execute("SELECT kunci, respons FROM idem"):
        IDEM[kunci] = bytes(respons)
    print(f"State dipulihkan dari {DB}: {len(BOOKING)} booking, {len(IDEM)} request_id", flush=True)


def kursi_valid(j, k):
    m = POLA_KURSI.match(k)
    return bool(m) and ord(m.group(1)) - 64 <= j.baris and int(m.group(2)) <= j.kolom


def terbitkan(jadwal_id, kursi, status, pemesan):
    ev = pb.PerubahanKursi(jadwal_id=jadwal_id, kursi=kursi, status=status, pemesan=pemesan, waktu_ms=int(time.time() * 1000))
    with SUBS_LOCK:
        for q in SUBS[jadwal_id]:
            try:
                q.put_nowait(ev)
            except queue.Full:
                pass                 # pemantau terlalu lambat: event dibuang agar server tidak tertahan


def balasan_terlambat():
    if LAMBAT and random.random() < LAMBAT:
        time.sleep(LAMBAT_DETIK)     # simulasi balasan terlambat: operasi SUDAH tersimpan


class LayananBioskop(pb_grpc.BioskopServicer):
    def ListJadwal(self, request, context):
        return pb.DaftarJadwal(jadwal=list(JADWAL.values()))

    def GetDenah(self, request, context):
        j = JADWAL.get(request.jadwal_id)
        if j is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"Jadwal {request.jadwal_id} tidak ada")
        with LOCK:
            terpesan = sorted(TERPESAN[j.id])
        return pb.Denah(jadwal_id=j.id, baris=j.baris, kolom=j.kolom, terpesan=terpesan)

    def PesanKursi(self, request, context):
        j = JADWAL.get(request.jadwal_id)
        if j is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"Jadwal {request.jadwal_id} tidak ada")
        kursi = list(dict.fromkeys(k.upper() for k in request.kursi))
        if not request.request_id or not request.pemesan or not kursi:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "request_id, pemesan, dan kursi wajib diisi")
        salah = [k for k in kursi if not kursi_valid(j, k)]
        if salah:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Kursi tidak valid: {salah}")
        kunci = "pesan:" + request.request_id
        with LOCK:
            lama = IDEM.get(kunci)
            if lama is not None:                                   # permintaan duplikat: kirim balasan lama
                return pb.PesanResponse.FromString(lama)
            diambil = [k for k in kursi if k in TERPESAN[j.id]]
            if diambil:
                hasil = pb.PesanResponse(sukses=False, pesan="Kursi sudah dipesan orang lain", kursi_gagal=diambil)
                conn.execute("INSERT INTO idem VALUES (?, ?)", (kunci, hasil.SerializeToString()))
            else:
                bid = "BK-" + uuid.uuid4().hex[:8]
                hasil = pb.PesanResponse(sukses=True, pesan=f"Booking {bid} berhasil", booking_id=bid)
                conn.execute("BEGIN IMMEDIATE")                    # booking + idem dalam satu transaksi
                conn.execute("INSERT INTO booking VALUES (?, ?, ?, ?)", (bid, j.id, request.pemesan, json.dumps(kursi)))
                conn.execute("INSERT INTO idem VALUES (?, ?)", (kunci, hasil.SerializeToString()))
                conn.execute("COMMIT")
                if CRASH:
                    os._exit(1)                                    # uji: crash setelah COMMIT, balasan belum terkirim
                BOOKING[bid] = {"jadwal_id": j.id, "pemesan": request.pemesan, "kursi": kursi}
                for k in kursi:
                    TERPESAN[j.id][k] = bid
                    terbitkan(j.id, k, "TERPESAN", request.pemesan)
            IDEM[kunci] = hasil.SerializeToString()
        balasan_terlambat()
        return hasil

    def BatalPesan(self, request, context):
        if not request.request_id or not request.booking_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "request_id dan booking_id wajib diisi")
        kunci = "batal:" + request.request_id
        with LOCK:
            lama = IDEM.get(kunci)
            if lama is not None:
                return pb.BatalResponse.FromString(lama)
            b = BOOKING.get(request.booking_id)
            if b is None:
                hasil = pb.BatalResponse(sukses=False, pesan="Booking tidak ditemukan")
                conn.execute("INSERT INTO idem VALUES (?, ?)", (kunci, hasil.SerializeToString()))
            elif b["pemesan"] != request.pemesan:
                hasil = pb.BatalResponse(sukses=False, pesan="Booking milik pemesan lain")
                conn.execute("INSERT INTO idem VALUES (?, ?)", (kunci, hasil.SerializeToString()))
            else:
                hasil = pb.BatalResponse(sukses=True, pesan=f"Booking {request.booking_id} dibatalkan")
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("DELETE FROM booking WHERE booking_id=?", (request.booking_id,))
                conn.execute("INSERT INTO idem VALUES (?, ?)", (kunci, hasil.SerializeToString()))
                conn.execute("COMMIT")
                del BOOKING[request.booking_id]
                for k in b["kursi"]:
                    del TERPESAN[b["jadwal_id"]][k]
                    terbitkan(b["jadwal_id"], k, "DIBATALKAN", b["pemesan"])
            IDEM[kunci] = hasil.SerializeToString()
        balasan_terlambat()
        return hasil

    def PantauKursi(self, request, context):
        j = JADWAL.get(request.jadwal_id)
        if j is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"Jadwal {request.jadwal_id} tidak ada")
        antrean = queue.Queue(maxsize=1000)
        with LOCK:                                                 # snapshot dan langganan atomik: tidak ada event terlewat
            awal = [(k, BOOKING[b]["pemesan"]) for k, b in TERPESAN[j.id].items()]
            with SUBS_LOCK:
                SUBS[j.id].append(antrean)
        try:
            for k, pemesan in sorted(awal):
                yield pb.PerubahanKursi(jadwal_id=j.id, kursi=k, status="TERPESAN", pemesan=pemesan,
                                        waktu_ms=int(time.time() * 1000), snapshot=True)
            while context.is_active():
                try:
                    yield antrean.get(timeout=0.5)
                except queue.Empty:
                    continue
        finally:
            with SUBS_LOCK:
                SUBS[j.id].remove(antrean)


def serve():
    init_db()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=WORKERS))
    pb_grpc.add_BioskopServicer_to_server(LayananBioskop(), server)
    server.add_insecure_port(f"[::]:{PORT}")
    server.start()
    print(f"Server bioskop siap di port {PORT} (workers={WORKERS})", flush=True)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
