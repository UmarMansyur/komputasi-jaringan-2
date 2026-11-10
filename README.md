# Dokumentasi Tugas 2 KBJ
## Job Queue dan Message Brokering Menggunakan Celery dan RabbitMQ

### 1. Deskripsi Proyek

Proyek ini menerapkan sistem antrean tugas (*job queue*) dan pertukaran pesan (*message brokering*) pada kasus perbankan digital. Sistem menggunakan **Celery** untuk menjalankan tugas secara asinkron dan **Kombu** untuk mengatur pengiriman pesan melalui *exchange* dan *queue*.

Implementasi dibagi menjadi tiga bagian:
1. Pengujian pola pengiriman pesan menggunakan *Fanout*, *Topic*, dan *Wildcard*.
2. Penerapan Celery pada proses transaksi menggunakan *Chain* dan *Group*.
3. Pembuatan *producer* sebagai pembangkit data transaksi.

### 2. Tugas 1 — Message Handling

Pengujian menggunakan dua jenis *exchange*, yaitu `fanout` dan `topic`.

#### 2.1 Konfigurasi Exchange dan Queue

Sistem menggunakan konfigurasi berikut:

| Komponen | Konfigurasi | Fungsi |
|---|---|---|
| `broadcast` | `fanout` | Mengirim pesan ke seluruh queue yang terhubung |
| `bank` | `topic` | Mengirim pesan berdasarkan pola routing key |
| `q_validasi` | `transaksi.validasi` | Menerima pesan validasi |
| `q_transfer` | `transaksi.transfer` | Menerima pesan transfer |
| `q_rekap` | `keuangan.rekap` | Menerima pesan rekap keuangan |
| `q_notifikasi` | `broadcast` | Menerima pesan notifikasi |

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

### 3. Tugas 2 — Penerapan Celery

Pada bagian ini, proses transaksi dikembangkan menggunakan Celery. Setiap tugas dijalankan secara asinkron dan dapat disusun menggunakan *Chain* maupun *Group*.

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

### 4. Tugas 3 — Producer sebagai Input Generator

Bagian ini menyediakan program `producer.py` untuk menghasilkan data transaksi secara otomatis. Data mencakup ID transaksi, nomor rekening, dan nominal transfer acak antara Rp50.000 hingga Rp1.000.000.

Parameter `--seed` dapat digunakan agar hasil pembangkitan data dapat diulang dengan nilai yang sama.

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

### 7. Kesimpulan

Proyek ini menunjukkan penerapan *message brokering* dan *job queue* pada simulasi transaksi perbankan digital. Pengujian membuktikan bahwa *topic exchange* mengatur pesan berdasarkan pola routing key, sedangkan *fanout exchange* menyebarkan pesan ke seluruh queue yang terhubung.

Celery digunakan untuk menjalankan proses transaksi secara berurutan melalui *Chain* dan secara paralel melalui *Group*. Sementara itu, `producer.py` menyediakan data transaksi otomatis untuk pengujian.

Pengujian menggunakan `memory://` hanya membuktikan proses melalui transportasi pesan dalam memori. Pengujian integrasi dengan RabbitMQ memerlukan broker dan worker yang aktif.