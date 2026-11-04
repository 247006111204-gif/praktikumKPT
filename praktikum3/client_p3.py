import time
import uuid

import grpc

import akademik_pb2 as pb


def transfer_dengan_retry(stub, dari, ke, jumlah, maks=3, id_baru_tiap_percobaan=False, log=print):
    rid = str(uuid.uuid4())                       # ID sama untuk setiap percobaan
    for percobaan in range(1, maks + 1):
        if id_baru_tiap_percobaan:
            rid = str(uuid.uuid4())               # L3.2: ID baru -> server menganggapnya permintaan baru
        try:
            req = pb.TransferRequest(request_id=rid, dari=dari, ke=ke, jumlah=jumlah)
            return stub.Transfer(req, timeout=2.0)
        except grpc.RpcError as e:
            bisa_ulang = e.code() in (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE)
            if not bisa_ulang or percobaan == maks:
                raise
            jeda = 0.5 * 2 ** (percobaan - 1)
            log(f"  percobaan {percobaan} gagal ({e.code().name}), ulang dalam {jeda} s")
            time.sleep(jeda)


def baca_saldo(stub):
    """Transfer bernilai 0 dengan ID baru: tidak mengubah saldo, hanya membaca isi saldo dari pesan balasan."""
    for _ in range(10):
        try:
            r = stub.Transfer(pb.TransferRequest(request_id=str(uuid.uuid4()), dari="A", ke="B", jumlah=0), timeout=10)
            return eval(r.pesan.split("Saldo kini ")[1].split(" (")[0])
        except grpc.RpcError:
            time.sleep(0.5)
    raise RuntimeError("tidak bisa membaca saldo")
