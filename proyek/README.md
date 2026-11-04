# Reservasi Tiket Bioskop berbasis gRPC (Tugas Proyek Mini RPC, studi kasus 1)

Beberapa client memesan kursi bioskop bersamaan; server menjamin satu kursi hanya dimiliki satu pemesan,
menyaring permintaan ganda (retry) dengan `request_id`, menyimpan state di SQLite (tahan crash),
dan menyiarkan perubahan kursi secara real-time lewat server streaming.

## RPC (bioskop.proto)
| RPC | Pola | Fungsi |
|---|---|---|
| ListJadwal | unary | daftar jadwal tayang |
| GetDenah | unary | kursi yang sudah terpesan |
| PesanKursi | unary | pesan 1+ kursi (atomik) |
| BatalPesan | unary | batalkan booking |
| PantauKursi | **server streaming** | status kursi real-time |

## Instalasi
```bash
python3 -m venv venv
source venv/bin/activate            # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Generate stub
```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. bioskop.proto
```

## Menjalankan server dan client
```bash
# terminal 1
python server.py                    # port 50051; variabel: PORT, DB, WORKERS (bawaan 32)

# terminal 2
python demo_client.py jadwal
python demo_client.py pesan J1 Andi A1 A2
python demo_client.py denah J1
python demo_client.py batal BK-xxxxxxxx Andi
python demo_client.py pantau J1     # biarkan berjalan, pesan kursi dari terminal lain
```

## Demo pada 2 komputer
Di komputer server: `python server.py`. Di komputer client, ganti alamat server:
`python demo_client.py --server <IP-SERVER>:50051 jadwal` (pastikan port 50051 diizinkan firewall).

## Demo pada 2 container
```bash
docker compose up --build           # container "server" dan "client" pada jaringan yang sama
docker compose run --rm client python demo_client.py --server server:50051 pesan J1 Andi A1
```

## Pengujian
```bash
python uji_proyek.py                # 8 uji otomatis (server dijalankan sendiri pada port 50061)

python server.py &                  # untuk benchmark
python benchmark.py localhost:50051 800   # latensi dan throughput 1, 5, 20 client
```
