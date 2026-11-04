"""Library client: timeout, retry dengan exponential backoff + jitter, request_id tetap pada setiap retry."""
import random
import time
import uuid

import grpc

import bioskop_pb2 as pb
import bioskop_pb2_grpc as pb_grpc

BISA_DIULANG = (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE)


class KlienBioskop:
    def __init__(self, alamat="localhost:50051", timeout=2.0, maks_percobaan=4, log=None):
        self.channel = grpc.insecure_channel(alamat)
        self.stub = pb_grpc.BioskopStub(self.channel)
        self.timeout = timeout
        self.maks = maks_percobaan
        self.log = log or (lambda *_: None)
        self.jumlah_retry = 0

    def _panggil(self, nama, req):
        for percobaan in range(1, self.maks + 1):
            try:
                return getattr(self.stub, nama)(req, timeout=self.timeout)
            except grpc.RpcError as e:
                if e.code() not in BISA_DIULANG or percobaan == self.maks:
                    raise
                jeda = 0.2 * 2 ** (percobaan - 1)
                jeda += random.uniform(0, jeda / 2)           # jitter agar retry banyak client tidak serentak
                self.jumlah_retry += 1
                self.log(f"{nama}: percobaan {percobaan} gagal ({e.code().name}), ulang dalam {jeda:.2f} s")
                time.sleep(jeda)

    def list_jadwal(self):
        return self._panggil("ListJadwal", pb.Kosong()).jadwal

    def denah(self, jadwal_id):
        return self._panggil("GetDenah", pb.DenahRequest(jadwal_id=jadwal_id))

    def pesan(self, jadwal_id, pemesan, kursi, request_id=None):
        rid = request_id or str(uuid.uuid4())                 # dibuat SEKALI; sama untuk semua retry
        return self._panggil("PesanKursi", pb.PesanRequest(request_id=rid, jadwal_id=jadwal_id, pemesan=pemesan, kursi=kursi))

    def batal(self, booking_id, pemesan, request_id=None):
        rid = request_id or str(uuid.uuid4())
        return self._panggil("BatalPesan", pb.BatalRequest(request_id=rid, booking_id=booking_id, pemesan=pemesan))

    def pantau(self, jadwal_id):
        """Generator event kursi (streaming; tidak di-retry otomatis, pemanggil dapat memanggil ulang)."""
        return self.stub.PantauKursi(pb.PantauRequest(jadwal_id=jadwal_id))

    def tutup(self):
        self.channel.close()
