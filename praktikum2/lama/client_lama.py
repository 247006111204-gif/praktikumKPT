# Client LAMA: stub dihasilkan dari akademik.proto TANPA field prodi (belum digenerate ulang).
import grpc

import akademik_pb2 as pb
import akademik_pb2_grpc as pb_grpc

with grpc.insecure_channel("localhost:50051") as channel:
    stub = pb_grpc.LayananNilaiStub(channel)
    mhs = stub.GetMahasiswa(pb.MahasiswaRequest(nim="2023001"))
    print(f"client lama menerima: nama={mhs.nama}, ipk={mhs.ipk}")
    print("field terdefinisi di stub lama:", [f.name for f in pb.Mahasiswa.DESCRIPTOR.fields])
    print("byte tak dikenal (field prodi) dipertahankan:", len(mhs.SerializeToString()), "byte setelah serialisasi ulang")
