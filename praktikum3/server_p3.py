import contextlib
import os
import random
import threading
import time
from concurrent import futures

import grpc

import akademik_pb2 as pb
import akademik_pb2_grpc as pb_grpc

PORT = os.environ.get("PORT", "50051")
WORKERS = int(os.environ.get("WORKERS", "10"))
DELAY = float(os.environ.get("DELAY", "0"))          # time.sleep di GetMahasiswa (L3.4)
LAMBAT = float(os.environ.get("LAMBAT", "0.3"))      # peluang balasan terlambat 3 detik
TANPA_LOCK = os.environ.get("TANPA_LOCK") == "1"     # L3.3: menghapus "with LOCK"
IO = float(os.environ.get("IO", "0"))                # simulasi akses penyimpanan di tengah transfer

SALDO = {"A": 1000.0, "B": 500.0}
PROSES = {}                 # request_id -> StatusResponse yang sudah dikirim
LOCK = threading.Lock()
DATA = {"2023001": ("Andi", 3.26)}


class LayananNilai(pb_grpc.LayananNilaiServicer):
    def GetMahasiswa(self, request, context):
        time.sleep(DELAY)
        nama, ipk = DATA[request.nim]
        return pb.Mahasiswa(nim=request.nim, nama=nama, ipk=ipk)

    def Transfer(self, request, context):
        kunci = contextlib.nullcontext() if TANPA_LOCK else LOCK    # <- "with LOCK:" pada modul
        with kunci:
            if request.request_id in PROSES:                          # permintaan duplikat
                return PROSES[request.request_id]
            if SALDO[request.dari] < request.jumlah:
                hasil = pb.StatusResponse(sukses=False, pesan="Saldo tidak cukup")
            else:
                if IO == 0:                                           # persis seperti modul
                    SALDO[request.dari] -= request.jumlah
                    SALDO[request.ke] += request.jumlah
                else:                                                 # baca-jeda-tulis: simulasi I/O 1 ms
                    dari = SALDO[request.dari]
                    time.sleep(IO)
                    SALDO[request.dari] = dari - request.jumlah
                    ke = SALDO[request.ke]
                    time.sleep(IO)
                    SALDO[request.ke] = ke + request.jumlah
                hasil = pb.StatusResponse(sukses=True, pesan=f"Saldo kini {SALDO}")
            PROSES[request.request_id] = hasil
        if random.random() < LAMBAT:
            time.sleep(3)  # simulasi balasan terlambat: transfer SUDAH terjadi
        return hasil


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=WORKERS))
    pb_grpc.add_LayananNilaiServicer_to_server(LayananNilai(), server)
    server.add_insecure_port(f"[::]:{PORT}")
    server.start()
    print(f"Server P3 port {PORT} workers={WORKERS} delay={DELAY} lambat={LAMBAT} tanpa_lock={TANPA_LOCK} io={IO}", flush=True)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
