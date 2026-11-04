"""CLI demo.  Contoh:
    python demo_client.py jadwal
    python demo_client.py denah J1
    python demo_client.py pesan J1 Andi A1 A2
    python demo_client.py batal BK-1234abcd Andi
    python demo_client.py pantau J1
"""
import argparse

import grpc

from klien import KlienBioskop


def main():
    ap = argparse.ArgumentParser(description="Client demo reservasi bioskop")
    ap.add_argument("--server", default="localhost:50051")
    sub = ap.add_subparsers(dest="perintah", required=True)
    sub.add_parser("jadwal")
    d = sub.add_parser("denah"); d.add_argument("jadwal_id")
    p = sub.add_parser("pesan"); p.add_argument("jadwal_id"); p.add_argument("pemesan"); p.add_argument("kursi", nargs="+")
    b = sub.add_parser("batal"); b.add_argument("booking_id"); b.add_argument("pemesan")
    m = sub.add_parser("pantau"); m.add_argument("jadwal_id")
    a = ap.parse_args()

    k = KlienBioskop(a.server, log=print)
    try:
        if a.perintah == "jadwal":
            for j in k.list_jadwal():
                print(f"{j.id}: {j.film} | {j.studio} | {j.waktu} | {j.baris} baris x {j.kolom} kursi")
        elif a.perintah == "denah":
            dn = k.denah(a.jadwal_id)
            print(f"{dn.jadwal_id}: {len(dn.terpesan)} kursi terpesan dari {dn.baris * dn.kolom}: {list(dn.terpesan)}")
        elif a.perintah == "pesan":
            r = k.pesan(a.jadwal_id, a.pemesan, a.kursi)
            print(("BERHASIL: " if r.sukses else "GAGAL: ") + r.pesan, list(r.kursi_gagal))
        elif a.perintah == "batal":
            r = k.batal(a.booking_id, a.pemesan)
            print(("BERHASIL: " if r.sukses else "GAGAL: ") + r.pesan)
        elif a.perintah == "pantau":
            print(f"Memantau {a.jadwal_id} (Ctrl+C untuk berhenti)...")
            for e in k.pantau(a.jadwal_id):
                print(f"{'[awal] ' if e.snapshot else ''}{e.kursi} {e.status} oleh {e.pemesan}")
    except grpc.RpcError as e:
        print("Error:", e.code().name, e.details())
    except KeyboardInterrupt:
        pass
    finally:
        k.tutup()


if __name__ == "__main__":
    main()
