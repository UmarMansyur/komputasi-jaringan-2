# Tugas 2 KBJ: Job Queue & Message Brokering (Celery & RabbitMQ)

**Komputasi Berbasis Jaringan — S2 Informatika, Universitas Negeri Surabaya**
1. Batari Wahyu Pangesti (26051905021)
2. Muhammad Umar Mansyur (26051905003)

## 1. Deskripsi Proyek

Repositori ini berisi implementasi *job queue* menggunakan Celery dan RabbitMQ sebagai *message broker*. Repositori ini memakai Sistem Perbankan Digital sebagai studi kasus. Sistem ini menggambarkan proses transfer dana antar rekening melalui aplikasi perbankan. Nasabah memilih rekening tujuan. Nasabah memasukkan nominal transfer. Sistem memperoleh informasi rekening asal dari akun yang sedang digunakan. Sistem kemudian memeriksa saldo dan memvalidasi transaksi sebelum sistem memproses transfer.

Pekerjaan seperti pemrosesan transfer membutuhkan waktu lama. Pekerjaan seperti pencatatan mutasi membutuhkan waktu lama. Pekerjaan seperti pengiriman notifikasi membutuhkan waktu lama. Pekerjaan seperti pembuatan laporan membutuhkan waktu lama ketika sistem menjalankannya secara bersamaan. Celery mengatur tugas-tugas tersebut. RabbitMQ menampung tugas-tugas itu dalam antrean. Celery Worker mengambil tugas tersebut dan Worker menjalankannya di latar belakang secara asinkron. Aplikasi memberikan respons lebih cepat tanpa aplikasi menunggu seluruh proses selesai.

Proyek ini mengikuti empat tahapan komunikasi AMQP. Tahapan tersebut menjelaskan aliran pesan dari producer sampai worker.

1. Producer mengirimkan pesan kepada Exchange. Producer berada pada berkas `producer.py`. Producer menentukan exchange tujuan dan routing key tanpa producer menunjuk worker secara langsung.
2. Exchange meneruskan pesan kepada Queue. Exchange `bank` bertipe `topic` mencocokkan routing key dengan binding key. Exchange `notifikasi` bertipe `fanout` menyalin pesan kepada seluruh queue yang terhubung.
3. Queue menyimpan pesan sebelum Worker memproses pesan itu. Queue `q_validasi` menyimpan tugas validasi. Queue `q_transfer` menyimpan tugas transfer. Queue `q_rekap` menyimpan tugas rekapitulasi. Queue `q_notifikasi` menyimpan tugas notifikasi.
4. Worker memproses pesan dan Worker mengirimkan *acknowledgement* kepada RabbitMQ. Konfigurasi `task_acks_late=True` menunda pengakuan sampai tugas selesai. Mekanisme *retry* mengembalikan pekerjaan yang gagal ke antrean dengan batas `max_retries=3`. Konfigurasi `worker_prefetch_multiplier=1` membatasi pengambilan pesan agar Worker tidak menimbun pekerjaan berat.

Proyek ini menyediakan lima tugas pada berkas `tasks.py`. Program menghitung total debet dengan rumus `total_debet = nominal + biaya_admin (Rp2.500)`.

- Fungsi `validasi_transaksi` menjalankan tugas ringan sebagai analogi SHORT UTS. Fungsi ini memeriksa saldo, nominal positif, perbedaan rekening, dan batas limit Rp10.000.000.
- Fungsi `proses_transfer` menjalankan tugas sedang. Fungsi ini menghitung nominal, biaya admin, dan total debet.
- Fungsi `rekap_keuangan` menjalankan tugas berat sebagai analogi LONG UTS. Fungsi ini menghitung jumlah, total nominal, rata-rata, nominal minimum, nominal maksimum, serta distribusi kategori kecil/sedang/besar.
- Fungsi `notifikasi_transaksi` menjalankan tugas siaran. Fungsi ini mengirimkan satu pesan kepada semua kanal melalui fan-out.
- Fungsi `kirim_mutasi` menjalankan sambungan chain. Fungsi ini menerima output transfer dan menambahkan `mutasi_id` serta status `tercatat`.

Tabel berikut memetakan program UTS berbasis socket mentah ke program Tugas 2 berbasis Celery:

| Komponen | UTS | Tugas 2 | Keterangan |
|---|---|---|---|
| Broker | `broker.py` TCP:5000, pilih worker round-robin/least-requests | RabbitMQ + `task_routes`/`task_queues` di `celery_app.py` | seleksi manual → routing AMQP via `-Q` + `--concurrency` |
| Worker | `SHORT` echo, `LONG` prima/faktorial | `validasi_transaksi` cepat, `proses_transfer` sedang, `rekap_keuangan` berat | pola cepat-vs-berat sama, domain jadi perbankan |
| Client | JSON `request_id`/`application_id` via TCP + `\n` | `producer.py` via `delay`/`apply_async` + `task_id` | framing manual → Celery + backend `rpc://` |

## 2. Tugas 1 — Uji Message Handling (Fan-Out, Topic, Wildcard)

Konfigurasi pusat (`celery_app.py`):

```python
broadcast = Exchange("notifikasi", type="fanout")
bank = Exchange("bank", type="topic")
Queue("q_validasi", exchange=bank, routing_key="transaksi.validasi"),
Queue("q_transfer", exchange=bank, routing_key="transaksi.transfer"),
Queue("q_rekap", exchange=bank, routing_key="keuangan.rekap"),
Queue("q_notifikasi", exchange=broadcast),
```

Uji murni AMQP ada di `demo_routing.py` (Kombu, default `memory://` sehingga jalan tanpa server; tambah `--broker amqp://guest:guest@localhost:5672//` untuk RabbitMQ asli). Hasil terverifikasi di `hasil_demo.txt`.

### A. Topic — pencocokan eksak

- **Konsep:** exchange `topic` meneruskan pesan hanya jika routing key pesan **sama persis** dengan binding key queue. Kata dipisah titik, mis. `transaksi.transfer` = 2 kata.
- **Implementasi:** `Queue("demo_topic_transfer", routing_key="transaksi.transfer")` dan `Queue("demo_topic_validasi", routing_key="transaksi.validasi")` pada exchange `bank`; kirim 1 pesan `routing_key="transaksi.transfer"`. Task Celery memakai pola sama: `proses_transfer` → `exchange="bank", routing_key="transaksi.transfer"`.
- **Hasil:** `transaksi.transfer` terima **1** (`{'id':'TRX-001','nominal':500000}`), `transaksi.validasi` terima **0**. Terbukti tidak bocor ke jenis pekerjaan lain.
- **Penerapan:** tugas spesifik per worker khusus (worker validasi tidak menerima pekerjaan transfer; mis. worker kliring hanya dengar `kliring.masuk`).

### B. Wildcard — pencocokan pola di atas topic

- **Konsep:** masih exchange `topic`, tetapi binding memakai pola: `*` = tepat 1 kata, `#` = 0 kata atau lebih. Pola ditulis di sisi queue.
- **Implementasi:** `Queue("demo_wild_star", routing_key="transaksi.*")` dan `Queue("demo_wild_hash", routing_key="transaksi.#")`; kirim 3 pesan: `transaksi.transfer` (2 level), `transaksi.transfer.bifast` (3 level), `kliring.masuk`.
- **Hasil:** `transaksi.*` terima **1** (hanya `transaksi.transfer`); `transaksi.#` terima **2** (`transfer` + `transfer.bifast`); keduanya menolak `kliring.masuk`. Jadi `*` menyempitkan, `#` memperluas.
- **Penerapan:** satu worker fleksibel untuk sekelompok transaksi (mis. rekap langganan `transaksi.#` otomatis terima `transaksi.transfer`, `transaksi.validasi`, hingga `transaksi.transfer.bifast`).

### C. Fan-Out — broadcast, abaikan routing key

- **Konsep:** exchange `fanout` mengabaikan routing key sepenuhnya; setiap pesan digandakan ke **semua** queue ter-bind (publish/subscribe).
- **Implementasi:** `Exchange("notifikasi", type="fanout")`, `Queue("q_notifikasi", exchange=broadcast)` tanpa routing key; `notifikasi_transaksi` dikirim via `apply_async(..., exchange="notifikasi")`. Demo membuat `demo_fanout_A`/`demo_fanout_B` lalu publish dengan `routing_key="diabaikan.saja"`.
- **Hasil:** 1 pesan → A terima **1**, B terima **1**, isi identik (`{'notifikasi':'Transfer Rp500.000 berhasil!'}`). Terbukti disalin, bukan dibagi habis (beda dengan round-robin UTS).
- **Penerapan:** notifikasi massal (satu status transfer terkirim ke aplikasi mobile, email, dan log audit sekaligus).

### Ringkasan perbedaan

| Aspek | Fan-Out | Topic | Wildcard |
|---|---|---|---|
| Tipe exchange | `fanout` (`notifikasi`) | `topic` (`bank`) | `topic` + pola |
| Peran routing key | Diabaikan | Harus sama persis | Cocok pola `*`/`#` |
| Penerima 1 pesan | Semua queue (disalin) | Satu/sedikit yang cocok | Banyak yang polanya cocok |
| Contoh | tanpa key | `transaksi.transfer` | `transaksi.*` / `transaksi.#` |
| Analogi | Pengeras suara bank | Slip setoran ke teller tertentu | Grup email `transaksi-*@bank` |

## 3. Tugas 2 — Modifikasi Program UTS dengan Celery

Tiap task didaftarkan `tugas2.*` dengan `bind=True`, `max_retries=3`, dan `sleep` 0.2/1/2 detik agar beda ringan–berat terlihat di Flower. `task_acks_late=True` + `worker_prefetch_multiplier=1` menjaga distribusi adil saat job berat bercampur ringan.

**Chain** (`run_chain.py`): `chain(proses_transfer.s("8812340001","8856780501",500000) | kirim_mutasi.s())` → `total_debet=502500.0, status=sukses+mutasi, mutasi_id=MUT-8812340001-500000`. Menunjukkan task berurutan: output transfer menjadi input mutasi.

**Group**: 3 `proses_transfer` paralel (500rb, 250rb, 1jt) lalu `rekap` → `jumlah=3, total=1757500.0, rata2=585833.33`. Menunjukkan paralel + agregasi.

```bash
python run_chain.py          # eager, tanpa RabbitMQ
python run_chain.py --broker # via RabbitMQ (butuh worker hidup)
```

## 4. Tugas 3 — Producer sebagai Input Generator

`producer.py` membangkitkan data via `gen_transaksi` (id `TRX-001`, rekening `88...`, nominal acak 50rb–1jt, `--seed` agar reproduksibel). Pesan diterima broker dan dikerjakan worker.

| Mode | Metode | Butuh RabbitMQ/worker | Tujuan |
|---|---|---|---|
| `--eager` | `.apply()` langsung | Tidak | uji fungsi, demo laptop |
| Default (broker) | `delay` / `apply_async(exchange, routing_key)` | Ya | uji distribusi; gagal dengan pesan "Connection refused → nyalakan RabbitMQ atau pakai --eager" |

Contoh:

```bash
python producer.py --demo --eager  # 8 pesan: 5 transfer + validasi + rekap + notifikasi
python producer.py --app transfer --num 6
python producer.py --app notifikasi --pesan "Transfer Rp500.000 berhasil!"
```

## 5. Cara Menjalankan

```bash
python -m venv myenv && source myenv/bin/activate
pip install -r requirements.txt  # celery, flower, kombu
python demo_routing.py
python producer.py --demo --eager
python run_chain.py
```

Dengan RabbitMQ:

```bash
brew services start rabbitmq  # atau: docker run -d --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management
celery -A celery_app worker --loglevel=info -Q q_validasi,q_transfer -n w1@%h --concurrency=2
celery -A celery_app worker --loglevel=info -Q q_rekap,q_notifikasi -n w2@%h --concurrency=2
celery --broker=amqp://guest:guest@localhost:5672// flower --port=5555  # http://localhost:5555
python producer.py --demo
python demo_routing.py --broker amqp://guest:guest@localhost:5672//
python run_chain.py --broker
```

## 6. File

- `celery_app.py` — config Celery + exchange `bank` (topic) & `notifikasi` (fanout) + routing.
- `tasks.py` — 5 task perbankan (Tugas 2).
- `producer.py` — input generator (Tugas 3: `--eager`/broker).
- `demo_routing.py` — uji Fan-Out/Topic/Wildcard (Tugas 1).
- `run_chain.py` — demo `chain` + `group`.
- `hasil_demo.txt` — output demo terverifikasi.
- `requirements.txt` — `celery`, `flower`, `kombu`.

## 7. Kesimpulan

1. Tugas 1: topic memisah eksak (1 vs 0), `*` menyempitkan (1) sedangkan `#` memperluas (2), fan-out menyalin 1→2 identik.
2. Tugas 2: socket UTS termigrasi ke Celery; chain menghubungkan transfer→mutasi, group menjalankan paralel lalu rekap.
3. Tugas 3: producer membangkitkan data transaksi acak reproduksibel dan mengirim via broker atau eager.

Kombu = lapisan messaging Celery↔broker; Celery = pengelola task/worker/retry. `memory://` hanya simulasi lokal, bukan pengganti uji RabbitMQ asli. Semua klaim di atas merujuk pada output yang benar-benar dieksekusi (`hasil_demo.txt`).
