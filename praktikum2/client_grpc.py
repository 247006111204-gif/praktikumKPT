import grpc

import akademik_pb2 as pb
import akademik_pb2_grpc as pb_grpc

with grpc.insecure_channel("localhost:50051") as channel:
    stub = pb_grpc.LayananNilaiStub(channel)
    mhs = stub.GetMahasiswa(pb.MahasiswaRequest(nim="2023001"))
    print(f"{mhs.nama} - IPK {mhs.ipk}")
    print(stub.TambahNilai(pb.NilaiRequest(nim="2023001", matkul="Komputasi Paralel", nilai=88)).pesan)
    for n in stub.StreamNilai(pb.MahasiswaRequest(nim="2023001")):
        print(f"  {n.matkul}: {n.nilai}")
    try:
        stub.GetMahasiswa(pb.MahasiswaRequest(nim="9999"))
    except grpc.RpcError as e:
        print("Error:", e.code(), e.details())
