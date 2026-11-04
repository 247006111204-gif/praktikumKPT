import sys
import threading
import time

import grpc

import akademik_pb2 as pb
import akademik_pb2_grpc as pb_grpc

ALAMAT = "localhost:50051"


def l21():
    with grpc.insecure_channel(ALAMAT) as ch:
        stub = pb_grpc.LayananNilaiStub(ch)

        def hasilkan():
            for matkul, nilai in [("Sistem Operasi", 80), ("Kalkulus", 75), ("Statistika", 90)]:
                print(f"kirim: 2023002 {matkul} = {nilai}")
                yield pb.NilaiRequest(nim="2023002", matkul=matkul, nilai=nilai)

        resp = stub.KirimBanyakNilai(hasilkan())
        print("balasan server:", resp.sukses, "-", resp.pesan)
        print("nilai Sari kini:", [(n.matkul, n.nilai) for n in stub.StreamNilai(pb.MahasiswaRequest(nim="2023002"))])


def l22():
    cetak = threading.Lock()

    def peserta(nama, pesan_pesan):
        with grpc.insecure_channel(ALAMAT) as ch:
            stub = pb_grpc.LayananNilaiStub(ch)

            def kirim():
                for jeda, isi in pesan_pesan:
                    time.sleep(jeda)
                    with cetak:
                        print(f"[{nama}] kirim   : {isi}", flush=True)
                    yield pb.Pesan(pengirim=nama, isi=isi)
                time.sleep(1.0)  # tetap terhubung agar sempat menerima balasan

            for p in stub.Diskusi(kirim()):
                with cetak:
                    print(f"[{nama}] terima  : <{p.pengirim}> {p.isi}", flush=True)

    ani = threading.Thread(target=peserta, args=("Ani", [(0.3, "Halo, ada yang sudah kerjakan L2.2?"), (1.0, "Oke, kita bandingkan hasilnya.")]))
    budi = threading.Thread(target=peserta, args=("Budi", [(0.8, "Halo Ani, sudah selesai.")]))
    ani.start(); budi.start(); ani.join(); budi.join()


def l23():
    with grpc.insecure_channel(ALAMAT) as ch:
        mhs = pb_grpc.LayananNilaiStub(ch).GetMahasiswa(pb.MahasiswaRequest(nim="2023001"))
    print(f"client BARU menerima: nama={mhs.nama}, ipk={mhs.ipk}, prodi={mhs.prodi}")
    print("field di stub baru:", [f.name for f in pb.Mahasiswa.DESCRIPTOR.fields])


def l24():
    import xmlrpc.client
    mhs = pb.Mahasiswa(nim="2023001", nama="Andi", ipk=3.26, prodi="Informatika")
    biner = mhs.SerializeToString()
    xml = xmlrpc.client.dumps(({"nim": "2023001", "nama": "Andi", "ipk": 3.26, "prodi": "Informatika"},), methodresponse=True)
    mhs3 = pb.Mahasiswa(nim="2023001", nama="Andi", ipk=3.26)
    print(f"Protobuf (nim, nama, ipk, prodi): {len(biner)} byte")
    print(f"Protobuf (nim, nama, ipk)       : {len(mhs3.SerializeToString())} byte")
    print(f"XML-RPC <methodResponse> struct : {len(xml.encode())} byte")
    print(f"Rasio XML-RPC / Protobuf        : {len(xml.encode()) / len(biner):.1f}x")
    print("heksadesimal protobuf:", biner.hex(" "))


def l25():
    with grpc.insecure_channel(ALAMAT) as ch:
        stub = pb_grpc.LayananNilaiStub(ch)
        stub.GetMahasiswa(pb.MahasiswaRequest(nim="2023001"))  # pemanasan
        req = pb.MahasiswaRequest(nim="2023001")
        t0 = time.perf_counter()
        for _ in range(10):
            stub.GetMahasiswa(req)
        sinkron = (time.perf_counter() - t0) * 1000
        t0 = time.perf_counter()
        futures_ = [stub.GetMahasiswa.future(req) for _ in range(10)]
        hasil = [f.result() for f in futures_]
        asinkron = (time.perf_counter() - t0) * 1000
        print(f"10 permintaan sinkron berurutan : {sinkron:.0f} ms")
        print(f"10 permintaan asinkron (.future): {asinkron:.0f} ms")
        print(f"jumlah hasil dikumpulkan        : {len(hasil)}, contoh: {hasil[0].nama}, IPK {hasil[0].ipk}")
        print(f"percepatan                      : {sinkron / asinkron:.1f}x")


if __name__ == "__main__":
    globals()[sys.argv[1]]()
