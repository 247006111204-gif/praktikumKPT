import os
import queue
import threading
import time
from concurrent import futures

import grpc

import akademik_pb2 as pb
import akademik_pb2_grpc as pb_grpc

DELAY = float(os.environ.get("DELAY", "0"))      # jeda buatan per GetMahasiswa (dipakai L2.5 dan L3.4)
WORKERS = int(os.environ.get("WORKERS", "10"))   # max_workers ThreadPoolExecutor

DATA = {
    "2023001": {"nama": "Andi", "prodi": "Informatika", "nilai": {"Basis Data": 85.0, "Jaringan": 78.0}},
    "2023002": {"nama": "Sari", "prodi": "Sistem Informasi", "nilai": {"Basis Data": 92.0}},
}
SUBS = []                      # antrean pesan milik setiap peserta ruang diskusi
SUBS_LOCK = threading.Lock()


class LayananNilai(pb_grpc.LayananNilaiServicer):
    def GetMahasiswa(self, request, context):
        if DELAY:
            time.sleep(DELAY)
        mhs = DATA.get(request.nim)
        if mhs is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"NIM {request.nim} tidak ditemukan")
        daftar = list(mhs["nilai"].values())
        ipk = sum(daftar) / len(daftar) / 25 if daftar else 0.0  # konversi sederhana 0-100 ke 0-4
        return pb.Mahasiswa(nim=request.nim, nama=mhs["nama"], ipk=round(ipk, 2), prodi=mhs.get("prodi", ""))

    def TambahNilai(self, request, context):
        if not 0 <= request.nilai <= 100:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Nilai harus 0-100")
        mhs = DATA.setdefault(request.nim, {"nama": "(baru)", "nilai": {}})
        mhs["nilai"][request.matkul] = request.nilai
        return pb.StatusResponse(sukses=True, pesan="Nilai tersimpan")

    def StreamNilai(self, request, context):
        mhs = DATA.get(request.nim)
        if mhs is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "NIM tidak ditemukan")
        for matkul, nilai in mhs["nilai"].items():
            yield pb.Nilai(matkul=matkul, nilai=nilai)
            time.sleep(0.5)  # simulasi data dikirim bertahap

    # L2.1 client streaming: terima banyak NilaiRequest, balas satu StatusResponse
    def KirimBanyakNilai(self, request_iterator, context):
        jumlah = 0
        for req in request_iterator:
            if not 0 <= req.nilai <= 100:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Nilai {req.nilai} di luar 0-100")
            mhs = DATA.setdefault(req.nim, {"nama": "(baru)", "nilai": {}})
            mhs["nilai"][req.matkul] = req.nilai
            jumlah += 1
        return pb.StatusResponse(sukses=True, pesan=f"{jumlah} nilai tersimpan")

    # L2.2 bidirectional streaming: pesan satu peserta diteruskan ke semua peserta lain
    def Diskusi(self, request_iterator, context):
        antrean = queue.Queue()
        with SUBS_LOCK:
            SUBS.append(antrean)

        def baca():
            try:
                for pesan in request_iterator:
                    with SUBS_LOCK:
                        tujuan = [s for s in SUBS if s is not antrean]
                    for s in tujuan:
                        s.put(pesan)
            finally:
                antrean.put(None)

        threading.Thread(target=baca, daemon=True).start()
        try:
            while True:
                item = antrean.get()
                if item is None:
                    break
                yield item
        finally:
            with SUBS_LOCK:
                SUBS.remove(antrean)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=WORKERS))
    pb_grpc.add_LayananNilaiServicer_to_server(LayananNilai(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print(f"Server gRPC berjalan di port 50051 (max_workers={WORKERS}, DELAY={DELAY})...", flush=True)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
