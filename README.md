# Dokumentasi Tugas 2 KBJ
## Job Queue dan Message Brokering Menggunakan Celery dan RabbitMQ

### 1. Deskripsi Proyek

Proyek ini menerapkan sistem antrean tugas (*job queue*) dan pertukaran pesan (*message brokering*) pada kasus perbankan digital. Sistem menggunakan **Celery** untuk menjalankan tugas secara asinkron dan **Kombu** untuk mengatur pengiriman pesan melalui *exchange* dan *queue*.

Implementasi dibagi menjadi tiga bagian:
1. Pengujian pola pengiriman pesan menggunakan *Fanout*, *Topic*, dan *Wildcard*.
2. Penerapan Celery pada proses transaksi menggunakan *Chain* dan *Group*.
3. Pembuatan *producer* sebagai pembangkit data transaksi.

#### 1.1 Studi Kasus: Bank Digital "BankNusantara Mobile"

Studi kasus yang dipakai adalah **bank digital tanpa kantor cabang** (mirip Jenius / Blu / Bank Jago). Nasabah berinteraksi lewat aplikasi mobile untuk transfer antar-rekening bank yang sama dan antar-bank via BI-FAST.

Konteks bisnisnya:
- Setiap nasabah punya nomor rekening 10 digit, contoh akun utama pengujian: `8800001234` dengan saldo Rp2.000.000.
- Setiap transfer dikenakan **biaya admin Rp2.500**, limit per transaksi **Rp10.000.000**, dan total debet = nominal + biaya.
- Transaksi harus **validasi dulu** (cek nominal positif, rekening asal ≠ tujuan, limit, saldo cukup) sebelum dana dipindahkan.
- Setelah transfer sukses, sistem wajib **mencatat mutasi** (`MUT-<asal>-<nominal>`) dan **mengirim notifikasi ke semua kanal** (push mobile + email + SMS).
- Setiap malam, sistem menjalankan **rekap keuangan** (total, rata-rata, min/maks, distribusi kategori kecil <100rb / sedang ≤500rb / besar >500rb) untuk laporan ke OJK / manajemen risiko.

Kenapa butuh *job queue* + *brokering*? Karena karakteristik bebannya beda:
- Validasi = tugas **SHORT** (~0,2 dtk), harus cepat agar UX tidak lemot.
- Transfer = tugas **MEDIUM** (~1 dtk), melibatkan pemotongan saldo, tidak boleh blocking request HTTP.
- Rekap = tugas **LONG** (~2 dtk), memproses ratusan transaksi sekaligus, cocok dijalankan paralel/background.
- Notifikasi = tugas **broadcast**, satu event sukses harus disebar ke banyak consumer sekaligus.

Tanpa antrean, satu rekap berat bisa memblokir transfer nasabah lain. Dengan Celery + RabbitMQ, tiap jenis pekerjaan dipisah ke queue-nya sendiri (`q_validasi`, `q_transfer`, `q_rekap`, `q_notifikasi`) dan dikerjakan worker terpisah.

#### 1.2 Alur Bisnis End-to-End dan Pemetaan Teknis

Alur satu kali transfer nasabah:

```text
Nasabah (mobile app)
  -> [1] validasi_transaksi (transaksi.validasi) : cek saldo & limit
  -> [2] proses_transfer (transaksi.transfer)    : debet nominal + 2500
  -> [3] kirim_mutasi (chain dari [2])           : catat MUT-xxx
  -> [4] notifikasi_transaksi (fanout)           : broadcast "Transfer RpX berhasil!"
  -> [5] rekap_keuangan (keuangan.rekap, batch)  : agregasi malam hari
```

Pemetaan konsep ke studi kasus:

| Konsep Teknis | Peran di Bank Digital | Contoh di Kode |
|---|---|---|
| `topic` exchange `bank` | Memisahkan jenis pekerjaan perbankan berdasarkan *routing key* | `transaksi.validasi` → `q_validasi`, `transaksi.transfer` → `q_transfer`, `keuangan.rekap` → `q_rekap` |
| `fanout` exchange `notifikasi` | Satu event sukses disebar ke semua kanal notifikasi | `{"notifikasi": "Transfer Rp500.000 berhasil!"}` diterima `demo_fanout_A` (mobile) + `demo_fanout_B` (email) |
| Wildcard `transaksi.*` / `transaksi.#` | Fleksibilitas menambah kanal baru tanpa ganti exchange, misal `transaksi.transfer.bifast` untuk antar-bank | `*` = tepat 1 kata (hanya `transaksi.transfer`), `#` = 0..N kata (termasuk `transaksi.transfer.bifast`) |
| Celery *Chain* | Ketergantungan berurutan: mutasi baru bisa dicatat kalau transfer sudah sukses | `proses_transfer -> kirim_mutasi`, hasil `total_debet: 502500` jadi input mutasi |
| Celery *Group* | Paralelisasi: 3 nasabah transfer bersamaan (500rb, 250rb, 1jt), lalu direkap | `total: 1757500` = 1.750.000 pokok + 3×2500 biaya |
| *Producer* | Simulator nasabah: membangkitkan ID `TRX-001`, rekening acak `88xx`, nominal acak 50rb–1jt | `producer.py --demo --eager` = 5 transfer + 1 validasi + 1 rekap + 1 notifikasi |

### 2. Soal 1 — Message Handling (Fan-Out, Topic, Wildcard)

Menjawab: *"Uji cobalah konsep penanganan pesan dengan skema Fan-Out, Topic, Wildcard, lalu dokumentasikan secara singkat perbedaannya."*

Pengujian ada di `demo_routing.py` (default `memory://`, bisa `--broker amqp://...` untuk RabbitMQ asli). Cara jalan:

```bash
python demo_routing.py
python demo_routing.py --broker amqp://guest:guest@localhost:5672//
```

Pengujian menggunakan dua jenis *exchange*, yaitu `fanout` dan `topic`.

#### 2.1 Konfigurasi Exchange dan Queue

Sistem menggunakan konfigurasi berikut:

| Komponen | Konfigurasi | Fungsi |
|---|---|---|
| `notifikasi` | `fanout` | Mengirim pesan ke seluruh queue yang terhubung (broadcast notifikasi) |
| `bank` | `topic` | Mengirim pesan berdasarkan pola routing key |
| `q_validasi` | `transaksi.validasi` | Menerima pesan validasi |
| `q_transfer` | `transaksi.transfer` | Menerima pesan transfer |
| `q_rekap` | `keuangan.rekap` | Menerima pesan rekap keuangan |
| `q_notifikasi` | `notifikasi` (fanout) | Menerima pesan notifikasi |

#### 2.2 Topic Routing

*Topic exchange* mengarahkan pesan berdasarkan kesesuaian pola *routing key* dengan aturan pengikatan (*binding*) queue.

Pengujian menggunakan dua queue:
- `demo_topic_transfer` dengan pola `transaksi.transfer`.
- `demo_topic_validasi` dengan pola `transaksi.validasi`.

Pesan dikirim menggunakan routing key `transaksi.transfer`. Hasilnya, hanya `demo_topic_transfer` yang menerima pesan dengan data:

```python
{
    "id": "TRX-001",
    "nominal": 500000
}
```

Queue validasi tidak menerima pesan karena pola routing key tidak sesuai.

#### 2.3 Wildcard Routing

Pengujian wildcard menggunakan dua pola:

| Pola | Hasil |
|---|---|
| `transaksi.*` | Menerima `transaksi.transfer` |
| `transaksi.#` | Menerima `transaksi.transfer` dan `transaksi.transfer.bifast` |
| Keduanya | Menolak `kliring.masuk` |

Pada *topic exchange*, simbol `*` mewakili tepat satu bagian kata, sedangkan `#` mewakili nol atau lebih bagian kata. Hasil tersebut menunjukkan bahwa pola `transaksi.#` memiliki jangkauan penerimaan lebih luas daripada `transaksi.*`.

#### 2.4 Fanout Exchange

Berbeda dengan *topic*, *fanout exchange* mengirim salinan pesan ke seluruh queue yang terhubung tanpa mempertimbangkan routing key.

Pengujian menggunakan dua queue, yaitu `demo_fanout_A` dan `demo_fanout_B`. Keduanya menerima pesan yang sama:

```python
{
    "notifikasi": "Transfer Rp500.000 berhasil!"
}
```

Hasil ini membuktikan bahwa satu pesan yang diterbitkan melalui *fanout exchange* dapat diterima oleh beberapa queue sekaligus.

#### 2.5 Ringkasan Perbedaan (jawaban singkat soal 1)

| Aspek | Fan-Out (`notifikasi`) | Topic eksak (`bank`) | Wildcard (`bank` + `*` / `#`) |
|---|---|---|---|
| Seleksi penerima | Semua bound queue, `routing_key` **diabaikan** | Hanya queue yang `binding == routing_key` persis | Queue yang polanya **cocok** (`*` = tepat 1 kata, `#` = 0..N kata) |
| Uji kirim | 1 pesan → A=1, B=1 | `transaksi.transfer` → transfer=1, validasi=0 | `transaksi.transfer`→ `*`=1, `#`=1; `.bifast`→ `*`=0, `#`=1; `kliring.masuk`→ 0,0 |
| Contoh bank | 1 event "Transfer berhasil" ke mobile + email + SMS | `transaksi.transfer` ke worker transfer, `transaksi.validasi` ke worker validasi | `transaksi.*` untuk kanal sesama-level, `transaksi.#` untuk mencakup sub-kanal baru mis. `transaksi.transfer.bifast` tanpa tambah exchange |
| Kapan dipakai | Broadcast / notifikasi | Routing tegas per jenis pekerjaan | Perluasan fleksibel tanpa ubah publisher |

Cara baca: Fan-Out = 1→semua, Topic = 1→1 yang cocok persis, Wildcard = 1→N yang polanya cocok.

### 3. Soal 2 — Modifikasi Program UTS dengan Celery

Menjawab: *"Lakukan modifikasi pada program UTS Anda dengan mengakomodasi konsep-konsep pada pustaka Celery!"*

Program UTS (`task/`: `client.py`, `broker.py`, `worker.py` via socket TCP, strategi `round_robin` / `least_requests`, aplikasi `SHORT` echo dan `LONG` prime/faktorial) dimodifikasi menjadi program Tugas 2 (`celery_app.py`, `tasks.py`, `run_chain.py`) dengan pemetaan berikut:

| UTS (socket manual) | Tugas 2 (Celery + RabbitMQ/Kombu) | Keterangan |
|---|---|---|
| `broker.py` socket port 5000 + `select_worker()` | RabbitMQ sebagai broker + `task_routes` + `Exchange`/`Queue` di `celery_app.py` | Pemilihan worker manual diganti routing AMQP deklaratif |
| `round_robin` / `least_requests` manual | `task_acks_late=True` + `worker_prefetch_multiplier=1` + worker `-Q` / `--concurrency` | Fair dispatch diserahkan ke broker; worker ambil 1 tugas selesai-dulu-ack |
| `client.py --app SHORT/LONG/MIXED` | `tasks.py`: `validasi_transaksi` (SHORT ~0,2 dtk), `proses_transfer` (MEDIUM ~1 dtk), `rekap_keuangan` (LONG ~2 dtk) + `notifikasi_transaksi`, `kirim_mutasi` | Beban SHORT/LONG dipertahankan, tapi dibungkus konteks bank + kategori kecil/sedang/besar |
| Thread per koneksi di `worker.py` | `@app.task(bind=True, max_retries=3)` + `self.retry(countdown=5)` | Retry otomatis Celery menggantikan try-next-worker manual |
| Respons socket JSON 1 baris | `chain` / `group` + result backend `rpc://` + Flower `:5555` | Komposisi alur + monitoring bawaan Celery |

Pada bagian ini, proses transaksi dikembangkan menggunakan Celery. Setiap tugas dijalankan secara asinkron dan dapat disusun menggunakan *Chain* maupun *Group*.

Konsep Celery yang diakomodasi (semua sudah di kode, bukan sekadar config):
- `@app.task(bind=True, max_retries=3)` + `self.retry()` di `validasi_transaksi`, `proses_transfer`, `rekap_keuangan` (`tasks.py:17,49,68`).
- `task_acks_late=True` + `worker_prefetch_multiplier=1` (`celery_app.py:21-22`) agar worker mati di tengah tugas tidak menghilangkan pesan.
- Routing eksplisit `task_routes` + `task_queues` (`celery_app.py:23-49`): `transaksi.validasi`→`q_validasi`, `transaksi.transfer`→`q_transfer`, `keuangan.rekap`→`q_rekap`, fanout `notifikasi`→`q_notifikasi`.
- *Chain* (`run_chain.py:20-23`) dan *Group* + rekap susulan (`run_chain.py:27-36`).

Konfigurasi tugas mencakup:
- `bind=True` agar tugas dapat mengakses informasi eksekusinya.
- `max_retries=3` sebagai batas percobaan ulang.
- `task_acks_late=True` agar pengakuan penerimaan pesan dilakukan setelah tugas selesai.
- `worker_prefetch_multiplier=1` untuk membatasi jumlah tugas yang diambil terlebih dahulu oleh worker.

Catatan: `max_retries=3` merupakan batas percobaan ulang. Tugas harus memanggil mekanisme `self.retry()` atau menggunakan konfigurasi percobaan ulang otomatis agar pengulangan benar-benar terjadi.

#### 3.1 Chain

*Chain* menjalankan beberapa tugas secara berurutan. Hasil tugas sebelumnya menjadi masukan bagi tugas berikutnya.

Pengujian menggunakan dua proses:
1. `proses_transfer` memproses pengiriman dana.
2. `kirim_mutasi` mencatat hasil transaksi.

Perintah:

```bash
python run_chain.py
```

Hasil pengujian:

```text
total_debet : 502500.0
status      : sukses+mutasi
mutasi_id   : MUT-8812340001-500000
```

Total debet sebesar Rp502.500 berasal dari nominal transfer Rp500.000 dan biaya Rp2.500.

#### 3.2 Group

*Group* menjalankan beberapa tugas secara paralel. Pengujian memproses tiga transaksi dengan nominal berbeda:

| Transaksi | Nominal |
|---|---:|
| 1 | Rp500.000 |
| 2 | Rp250.000 |
| 3 | Rp1.000.000 |

Setelah seluruh tugas selesai, sistem menjalankan proses rekapitulasi.

Hasil pengujian:

```text
jumlah : 3
total  : 1757500.0
rata2  : 585833.33
```

Nilai total tersebut perlu diperiksa terhadap perhitungan biaya transaksi. Jumlah nominal pokok ketiga transfer adalah Rp1.750.000, sehingga selisih Rp7.500 perlu dipastikan berasal dari biaya yang diterapkan oleh program.

Untuk menjalankan pengujian menggunakan RabbitMQ:

```bash
python run_chain.py --broker
```

Mode tersebut memerlukan RabbitMQ dan worker Celery yang aktif.

### 4. Soal 3 — Producer sebagai Input Generator

Menjawab: *"Buatlah skrip yang bertindak sebagai producer. Producer tersebut merupakan pembangkit data masukan (input generator) yang pesannya akan diterima oleh broker dan dikerjakan oleh worker."*

Bagian ini menyediakan program `producer.py` untuk menghasilkan data transaksi secara otomatis. Data mencakup ID transaksi, nomor rekening, dan nominal transfer acak antara Rp50.000 hingga Rp1.000.000.

Peran tiap komponen (model broker–worker):
- **Producer** (`producer.py`): membangkitkan `TRX-xxx`, rekening `88xx`, nominal acak; menerbitkan pesan via `apply_async(..., exchange, routing_key)` (mode broker) atau `apply()` (mode eager tanpa broker).
- **Broker** (RabbitMQ, `celery_app.py:7`): menerima pesan di exchange `bank` (topic) / `notifikasi` (fanout) lalu mengantrekannya ke `q_validasi` / `q_transfer` / `q_rekap` / `q_notifikasi` sesuai routing key.
- **Worker** (`celery -A celery_app worker -Q ...`): mengambil pesan dari queue-nya dan mengerjakan `tasks.py` (`validasi_transaksi`, `proses_transfer`, `rekap_keuangan`, `notifikasi_transaksi`).

Parameter `--seed` dapat digunakan agar hasil pembangkitan data dapat diulang dengan nilai yang sama (contoh di `hasil_demo.txt` memakai `--seed 7`).

Producer menyediakan dua mode:

- **Eager:** tugas dijalankan langsung menggunakan `.apply()`, tanpa RabbitMQ dan worker.
- **Broker:** pesan dikirim melalui RabbitMQ menggunakan `delay()` atau `apply_async()`.

Contoh penggunaan:

**Menjalankan seluruh skenario dalam mode eager:**

```bash
python producer.py --demo --eager
```

Perintah ini menghasilkan delapan pesan, terdiri atas lima transfer, satu validasi, satu rekap, dan satu notifikasi.

**Menghasilkan enam transaksi transfer:**

```bash
python producer.py --app transfer --num 6
```

**Mengirim notifikasi:**

```bash
python producer.py --app notifikasi --pesan "Transfer Rp500.000 berhasil!"
```

### 5. Cara Menjalankan Program

#### 5.1 Persiapan Lingkungan

Buat lingkungan Python dan pasang seluruh dependensi:

```bash
python -m venv myenv
source myenv/bin/activate
pip install -r requirements.txt
```

#### 5.2 Menjalankan RabbitMQ

RabbitMQ dapat dijalankan melalui Homebrew:

```bash
brew services start rabbitmq
```

Atau menggunakan Docker:

```bash
docker run -d --name rabbit \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management
```

#### 5.3 Menjalankan Worker

Worker dibagi menjadi dua kelompok berdasarkan queue:

```bash
celery -A celery_app worker --loglevel=info \
  -Q q_validasi,q_transfer -n w1@%h --concurrency=2
```

```bash
celery -A celery_app worker --loglevel=info \
  -Q q_rekap,q_notifikasi -n w2@%h --concurrency=2
```

#### 5.4 Menjalankan Program

```bash
python demo_routing.py
python producer.py --demo --eager
python run_chain.py
```

Untuk memantau aktivitas worker, gunakan Flower:

```bash
celery --broker=amqp://guest:guest@localhost:5672// flower --port=5555
```

Buka alamat berikut melalui browser:

```text
http://localhost:5555
```

### 6. Struktur dan Fungsi Berkas

| Berkas | Fungsi |
|---|---|
| `celery_app.py` | Konfigurasi Celery, broker, exchange, dan queue |
| `tasks.py` | Definisi lima tugas perbankan |
| `producer.py` | Pembangkitan dan pengiriman data transaksi |
| `demo_routing.py` | Pengujian Fanout, Topic, dan Wildcard |
| `run_chain.py` | Pengujian Chain dan Group |
| `hasil_demo.txt` | Dokumentasi hasil pengujian |
| `requirements.txt` | Daftar pustaka yang diperlukan |

### 7. Kesimpulan (jawaban 3 soal)

1. **Soal 1 — Message handling:** `demo_routing.py` membuktikan Fan-Out menyalin 1 pesan ke semua queue (A=1, B=1, routing key diabaikan, cocok untuk notifikasi broadcast); Topic eksak hanya mengantar ke queue yang binding-nya sama persis (`transaksi.transfer`=1, `transaksi.validasi`=0); Wildcard `*` (tepat 1 kata) lebih sempit daripada `#` (0..N kata: menerima `transaksi.transfer` dan `transaksi.transfer.bifast`, menolak `kliring.masuk`).
2. **Soal 2 — Modifikasi UTS ke Celery:** broker socket + `round_robin`/`least_requests` manual diganti RabbitMQ + `task_routes`/`Exchange`/`Queue`; `SHORT`/`LONG` menjadi `validasi_transaksi`/`proses_transfer`/`rekap_keuangan` dengan `bind=True`, `max_retries=3` + `self.retry()`, `acks_late`, `prefetch=1`; alur UTS sekuensial/paralel kini dinyatakan sebagai `chain(proses_transfer → kirim_mutasi)` (total_debet 502.500) dan `group(3 transfer)` + rekap (total 1.757.500).
3. **Soal 3 — Producer:** `producer.py` adalah input generator (`TRX-xxx`, rekening `88xx`, nominal 50rb–1jt, `--seed` untuk reproduksibel); pesannya diterima broker (exchange `bank`/`notifikasi` → queue `q_*`) dan dikerjakan worker Celery (`-Q q_validasi,q_transfer` / `-Q q_rekap,q_notifikasi`), dengan mode `--eager` untuk uji tanpa RabbitMQ dan mode broker (`delay`/`apply_async`) untuk integrasi penuh.

Pengujian menggunakan `memory://` hanya membuktikan proses melalui transportasi pesan dalam memori. Pengujian integrasi dengan RabbitMQ memerlukan broker dan worker yang aktif.