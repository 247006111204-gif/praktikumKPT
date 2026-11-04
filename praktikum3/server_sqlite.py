# Varian server untuk pertanyaan analisis: saldo disimpan di SQLite (tahan restart).
# MODE=campur : daftar PROSES tetap di memori (seperti server_p3.py)  -> at-most-once rusak setelah restart
# MODE=sqlite : PROSES disimpan di SQLite dalam transaksi yang sama dengan perubahan saldo
import os
import sqlite3
import threading
from concurrent import futures

import grpc

import akademik_pb2 as pb
import akademik_pb2_grpc as pb_grpc

MODE = os.environ.get("MODE", "sqlite")
DB = os.environ.get("DB", "p3.db")
CRASH = os.environ.get("CRASH") == "1"     # matikan proses tepat setelah COMMIT, sebelum balasan terkirim
LOCK = threading.Lock()
PROSES = {}


def koneksi():
    return sqlite3.connect(DB, timeout=10, isolation_level=None)


def init():
    c = koneksi()
    c.execute("CREATE TABLE IF NOT EXISTS saldo(akun TEXT PRIMARY KEY, nilai REAL)")
    c.execute("CREATE TABLE IF NOT EXISTS proses(request_id TEXT PRIMARY KEY, sukses INTEGER, pesan TEXT)")
    c.execute("INSERT OR IGNORE INTO saldo VALUES ('A', 1000), ('B', 500)")
    c.close()


class LayananNilai(pb_grpc.LayananNilaiServicer):
    def Transfer(self, request, context):
        with LOCK:
            c = koneksi()
            try:
                if MODE == "sqlite":
                    baris = c.execute("SELECT sukses, pesan FROM proses WHERE request_id=?", (request.request_id,)).fetchone()
                    if baris:
                        return pb.StatusResponse(sukses=bool(baris[0]), pesan=baris[1] + " (balasan lama, tidak dieksekusi ulang)")
                elif request.request_id in PROSES:
                    return PROSES[request.request_id]
                c.execute("BEGIN IMMEDIATE")
                saldo_dari = c.execute("SELECT nilai FROM saldo WHERE akun=?", (request.dari,)).fetchone()[0]
                if saldo_dari < request.jumlah:
                    sukses, pesan = False, "Saldo tidak cukup"
                else:
                    c.execute("UPDATE saldo SET nilai = nilai - ? WHERE akun=?", (request.jumlah, request.dari))
                    c.execute("UPDATE saldo SET nilai = nilai + ? WHERE akun=?", (request.jumlah, request.ke))
                    s = dict(c.execute("SELECT akun, nilai FROM saldo").fetchall())
                    sukses, pesan = True, f"Saldo kini {s}"
                if MODE == "sqlite":
                    c.execute("INSERT INTO proses VALUES (?, ?, ?)", (request.request_id, int(sukses), pesan))
                c.execute("COMMIT")
                if CRASH:
                    os._exit(1)           # simulasi crash: transfer tersimpan, balasan belum terkirim
                hasil = pb.StatusResponse(sukses=sukses, pesan=pesan)
                PROSES[request.request_id] = hasil
                return hasil
            finally:
                c.close()


def serve():
    init()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb_grpc.add_LayananNilaiServicer_to_server(LayananNilai(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print(f"Server SQLite MODE={MODE} CRASH={CRASH} siap", flush=True)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
