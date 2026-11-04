import sys
import grpc
import akademik_pb2 as pb
import akademik_pb2_grpc as pb_grpc
from client_p3 import baca_saldo

RID = "TRX-0001"          # ID yang sama dipakai sebelum dan sesudah server restart
with grpc.insecure_channel("localhost:50051") as ch:
    stub = pb_grpc.LayananNilaiStub(ch)
    if sys.argv[1] == "kirim":
        try:
            stub.Transfer(pb.TransferRequest(request_id=RID, dari="A", ke="B", jumlah=100), timeout=5)
        except grpc.RpcError as e:
            print("client: balasan tidak diterima ->", e.code().name, "(server crash setelah COMMIT)")
    else:
        r = stub.Transfer(pb.TransferRequest(request_id=RID, dari="A", ke="B", jumlah=100), timeout=5)
        print("client: retry setelah restart ->", r.pesan)
        s = baca_saldo(stub)
        print(f"saldo akhir: {s} | A berkurang {1000 - s['A']:.0f} (seharusnya 100)")
