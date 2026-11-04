import json
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import grpc

import akademik_pb2 as pb
import akademik_pb2_grpc as pb_grpc
from client_p3 import baca_saldo, transfer_dengan_retry


def _transfer_n(n, id_baru):
    with grpc.insecure_channel("localhost:50051") as ch:
        stub = pb_grpc.LayananNilaiStub(ch)
        for i in range(1, n + 1):
            r = transfer_dengan_retry(stub, "A", "B", 10, id_baru_tiap_percobaan=id_baru)
            print(f"transfer {i:2d}: sukses={r.sukses}")
        s = baca_saldo(stub)
        print(f"saldo akhir: {s} | total = {sum(s.values()):.0f} | A berkurang {1000 - s['A']:.0f}")


def l31():
    _transfer_n(20, id_baru=False)


def l32():
    _transfer_n(20, id_baru=True)


def l33():
    with grpc.insecure_channel("localhost:50051") as ch:
        stub = pb_grpc.LayananNilaiStub(ch)
        with ThreadPoolExecutor(max_workers=10) as pool:
            list(pool.map(lambda _: transfer_dengan_retry(stub, "A", "B", 10), range(50)))
        s = baca_saldo(stub)
        print(f"saldo akhir: {s} | total = {sum(s.values()):.0f} (seharusnya 1500) | A = {s['A']:.0f} (seharusnya 500)")


def l34():
    workers = sys.argv[2]
    n_req, n_thread = 200, 20
    lat = []
    lock = threading.Lock()
    with grpc.insecure_channel("localhost:50051") as ch:
        stub = pb_grpc.LayananNilaiStub(ch)
        stub.GetMahasiswa(pb.MahasiswaRequest(nim="2023001"))

        def satu(_):
            t = time.perf_counter()
            stub.GetMahasiswa(pb.MahasiswaRequest(nim="2023001"))
            d = (time.perf_counter() - t) * 1000
            with lock:
                lat.append(d)

        t0 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=n_thread) as pool:
            list(pool.map(satu, range(n_req)))
        total = time.perf_counter() - t0
    hasil = {"max_workers": int(workers), "waktu_total_s": round(total, 2),
             "throughput": round(n_req / total, 1), "latensi_ms": round(statistics.mean(lat))}
    print(json.dumps(hasil))


def l35():
    servers = ["localhost:50051", "localhost:50052"]
    idx = 0
    for i in range(1, 9):
        for _ in range(len(servers)):
            alamat = servers[idx]
            try:
                with grpc.insecure_channel(alamat) as ch:
                    m = pb_grpc.LayananNilaiStub(ch).GetMahasiswa(pb.MahasiswaRequest(nim="2023001"), timeout=1.0)
                print(f"panggilan {i}: dijawab {alamat} -> {m.nama}", flush=True)
                break
            except grpc.RpcError as e:
                print(f"panggilan {i}: {alamat} {e.code().name}, pindah ke server berikutnya", flush=True)
                idx = (idx + 1) % len(servers)
        time.sleep(1)


if __name__ == "__main__":
    globals()[sys.argv[1]]()
