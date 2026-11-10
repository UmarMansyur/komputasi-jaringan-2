# Tugas 2 KBJ: Job Queue & Message-Brokering (Celery & RabbitMQ)

**Mata Kuliah:** Komputasi Berbasis Jaringan (S2 Informatika, Universitas Negeri Surabaya)
**Anggota Kelompok:**

1. Batari Wahyu Pangesti (26051905021)
2. Muhammad Umar Mansyur  (26051905003)

---

## 1. Deskripsi Proyek

Repositori ini berisi implementasi *Job Queue* menggunakan arsitektur **Celery** dan **RabbitMQ** sebagai *message broker*. Proyek ini mensimulasikan proses latar belakang (*background processing*) pada sebuah **Sistem Akademik Sederhana** (sengaja dibuat mudah, hanya demo).

Ketika dosen menginput nilai satu kelas (misalnya 40 mahasiswa) lalu menekan "Hitung & Rekap", tugas-tugas berat — seperti menghitung Nilai Akhir satu per satu, merekap statistik kelas (rata-rata, distribusi grade A–E), dan menyebar pengumuman ke semua prodi — **tidak** diproses langsung di request web (sinkron) yang akan membuat halaman lama dimuat. Sebaliknya, request tersebut dilempar ke antrean (*broker*) lalu dikerjakan oleh *worker* secara **asinkron**.

Alur AMQP yang dipakai (sesuai PPT 3 Nov 2026):

1. *Messages are sent to exchanges* — producer tidak kirim langsung ke worker, melainkan ke *exchange* (`kampus` / `pengumuman`).
2. *An exchange routes messages to one or more queues* — tipe *exchange* (`topic` / `fanout`) menentukan aturan routing ke *queue*.
3. *The message waits in the queue until someone consumes it* — worker yang *subscribe* ke *queue* tertentu mengambil job saat siap (bisa diatur *concurrency*).
4. *The message is deleted when acknowledged* — dengan `task_acks_late=True`, job yang gagal (worker mati) kembali ke antrean dan bisa di-*retry* (`max_retries=3`).

Studi kasus demo (5 task di `tasks.py`):

- `echo_nama` — ringan (analogi UTS SHORT): `"Ani"` -> `"Halo, Ani!"`.
- `hitung_nilai_akhir` — sedang: `NA = 0.3*tugas + 0.3*UTS + 0.4*UAS` + grade A–E.
- `rekap_kelas` — berat (analogi UTS LONG): terima list nilai, hitung jumlah, rata-rata, min, maks, distribusi grade.
- `broadcast_pengumuman` — siaran: 1 pesan untuk semua worker.
- `tambah_predikat` — sambungan *chain*: terima output `hitung_nilai_akhir`, tambah `lulus` + `predikat`.

Pemetaan UTS (`task/`) ke Celery (`tugas2-celery/`):

| UTS (socket mentah) | Celery (Tugas 2) | Keterangan |
|---|---|---|
| `broker.py` — terima TCP port 5000, pilih worker round_robin / least_requests, forward via socket | RabbitMQ sebagai broker + `task_routes` / `task_queues` di `celery_app.py:36-62` | Seleksi worker manual diganti routing AMQP; *load balancing* diatur via `-Q` (queue yang dikonsumsi tiap worker) dan `--concurrency` |
| `worker.py` — `SHORT` echo cepat, `LONG` prima/faktorial berat | `tasks.py:26-92` — `echo_nama` cepat, `hitung_nilai_akhir` sedang, `rekap_kelas` berat | Pola sama (cepat vs berat), domain diganti akademik agar mudah dijelaskan |
| `client.py` — bangun JSON `request_id/application_id/payload`, kirim `\n` via TCP, cetak `worker_id` + distribusi | `producer.py` — bangkitkan data mahasiswa acak, kirim via `delay` / `apply_async(exchange, routing_key)`, cetak `task_id` + hasil | *Framing* newline + `worker_id` manual tidak diperlukan lagi; Celery + `rpc://` backend mengurus ID, status, dan hasil |

---

## 2. Eksplorasi Penanganan Pesan (Message Handling)

Konfigurasi exchange/queue pusat ada di `celery_app.py:21-23` dan `celery_app.py:57-62`:

```python
broadcast = Exchange("pengumuman", type="fanout")  # Fan-Out
kampus = Exchange("kampus", type="topic")          # Topic + Wildcard
Queue("q_echo", exchange=kampus, routing_key="teks.echo"),
Queue("q_nilai", exchange=kampus, routing_key="nilai.akhir"),
Queue("q_rekap", exchange=kampus, routing_key="nilai.rekap"),
Queue("q_pengumuman", exchange=broadcast),
```

Uji routing murni AMQP ada di `demo_routing.py` (pakai `memory://` sehingga jalan tanpa server; tambah `--broker amqp://...` untuk RabbitMQ asli). Hasil terverifikasi tersimpan di `hasil_demo.txt`.

### A. Topic (Pencocokan Pasti / Spesifik)

- **Konsep:** Pesan dirutekan dari *exchange* bertipe `topic` ke *queue* **hanya jika** `routing_key` pesan **sama persis** dengan *binding key* queue. Kata dipisah titik (misalnya `nilai.tugas` = 2 kata: `nilai` + `tugas`). Ini routing paling ketat dan paling mudah diprediksi.
- **Implementasi di kode:**
  - Queue dideklarasikan dengan binding eksak di `demo_routing.py` — `Queue("demo_topic_tugas", exchange=kampus, routing_key="nilai.tugas")` dan `Queue("demo_topic_uts", exchange=kampus, routing_key="nilai.uts")`.
  - Task Celery memakai routing eksak yang sama di `celery_app.py:42-46` (`hitung_nilai_akhir` -> `exchange="kampus", routing_key="nilai.akhir"`) dan `producer.py` mengirim dengan `apply_async(..., exchange="kampus", routing_key="nilai.akhir")`.
  - Kirim satu pesan `routing_key="nilai.tugas"`; broker menyalinnya hanya ke queue `nilai.tugas`.
- **Hasil demo (`hasil_demo.txt`):** kirim `nilai.tugas` -> queue `nilai.tugas` terima **1** pesan (`{'mhs': 'Ani', 'nilai': 80}`), queue `nilai.uts` terima **0**. Terbukti tidak ada bocor ke topik lain.
- **Penggunaan:** Cocok untuk tugas spesifik yang harus dikerjakan worker khusus dan tidak boleh tertukar. Contoh akademik: nilai Tugas dikerjakan worker A, nilai UTS oleh worker B, rekap kelas oleh worker C. Contoh umum: worker khusus transaksi keuangan hanya dengar `pesanan.pembayaran`.

### B. Wildcard (Pencocokan Pola di atas Topic)

- **Konsep:** Masih *exchange* bertipe `topic`, tetapi *binding key* memakai pola: `*` = **tepat 1 kata**, `#` = **0 kata atau lebih**. Pola ditulis di sisi *queue* (konsumen), bukan di sisi pesan. Satu pesan bisa diterima banyak queue sekaligus jika polanya cocok, tanpa menambah *exchange* baru.
- **Implementasi di kode:**
  - `demo_routing.py` mendeklarasikan `Queue("demo_wild_star", routing_key="nilai.*")` dan `Queue("demo_wild_hash", routing_key="nilai.#")` pada *exchange* `kampus` yang sama.
  - Tiga pesan dikirim: `nilai.tugas` (2 level), `nilai.tugas.remidi` (3 level), `absensi.masuk` (di luar `nilai.`).
- **Hasil demo (`hasil_demo.txt`):**
  - Binding `nilai.*` terima **1** pesan (`nilai.tugas` saja). Alasannya: `*` hanya cocok 2-level; `nilai.tugas.remidi` (3 level) **ditolak**, `absensi.masuk` (awalan beda) **ditolak**.
  - Binding `nilai.#` terima **2** pesan (`nilai.tugas` dan `nilai.tugas.remidi`). Alasannya: `#` cocok semua yang berawalan `nilai.` mau 2 level atau 3 level; `absensi.masuk` tetap **ditolak**.
  - Kesimpulan: `*` menyempitkan (tegas 1 level), `#` memperluas (semua turunan).
- **Penggunaan:** Cocok untuk menangkap kelompok tugas sejenis dengan satu worker fleksibel. Contoh akademik: worker rekap berlangganan `nilai.#` sehingga otomatis menerima `nilai.tugas`, `nilai.uts`, `nilai.uas`, bahkan `nilai.tugas.remidi` tanpa daftar satu per satu. Contoh umum: notifikasi berlangganan `pesanan.*` sehingga satu queue menerima `pesanan.dikirim`, `pesanan.sampai`, `pesanan.batal`.

### C. Fan-Out (Broadcast, Abaikan Routing Key)

- **Konsep:** *Exchange* bertipe `fanout` **mengabaikan `routing_key` sepenuhnya**. Setiap pesan yang masuk digandakan ke **semua** *queue* yang ter-*bind*, apa pun isi *routing_key*-nya (bahkan string sampah). Ini siaran satu-ke-banyak (*publish/subscribe*).
- **Implementasi di kode:**
  - Exchange `Exchange("pengumuman", type="fanout")` di `celery_app.py:22`; queue `q_pengumuman` di-bind tanpa *routing_key* (`celery_app.py:61`).
  - Task `broadcast_pengumuman` (`tasks.py:78-82`) dikirim via `apply_async(args=[pesan], exchange="pengumuman")` di `producer.py` — perhatikan tidak ada `routing_key`.
  - Demo `demo_routing.py` membuat dua queue (`demo_fanout_A`, `demo_fanout_B`) pada *exchange* yang sama lalu publish dengan `routing_key="diabaikan.saja"`.
- **Hasil demo (`hasil_demo.txt`):** 1 pesan dikirim -> queue A terima **1**, queue B terima **1**, isinya **identik** (`{'pengumuman': 'Kuliah daring besok!'}`). Terbukti disalin, bukan dibagi habis (beda dengan *round-robin* UTS yang 1 request hanya dikerjakan 1 worker).
- **Penggunaan:** Cocok untuk siaran massal yang wajib diterima semua pihak. Contoh akademik: pengumuman "Kuliah daring besok" / "UAS dimajukan" harus masuk ke worker tiap prodi, modul log, dan modul notifikasi sekaligus. Contoh umum: *push notification flash sale* ke aplikasi HP dan Web bersamaan (`queue_promo_hp` + `queue_promo_web`).

### Ringkasan perbedaan

| Aspek | Fan-Out | Topic | Wildcard |
|---|---|---|---|
| Tipe exchange | `fanout` (`pengumuman`) | `topic` (`kampus`) | `topic` (`kampus`) + pola |
| Peran `routing_key` | **Diabaikan** | Harus **sama persis** | Dicocokkan dengan **pola** |
| Jumlah penerima 1 pesan | **Semua** queue ter-bind (disalin) | **Satu** (atau sedikit) queue yang cocok | **Banyak** queue yang polanya cocok |
| Contoh binding | `(tanpa key)` | `nilai.tugas` | `nilai.*` / `nilai.#` |
| Contoh pesan cocok | apa pun | `nilai.tugas` saja | `*`: `nilai.tugas` ya, `nilai.tugas.remidi` tidak; `#`: keduanya ya |
| Analogi | Pengeras suara kampus | Surat ke alamat lengkap | Grup email `nilai-*@kampus` |
| Kapan dipakai | Pengumuman / invalidasi cache global | Tugas spesifik per worker | Agregasi / notifikasi sekelompok event |

---

## 3. Modifikasi Program UTS dengan Celery + Producer (Input Generator)

**Task (`tasks.py`):** tiap task diberi nama (`tugas2.*`), `bind=True`, `max_retries=3`, dan simulasi beban (`sleep` 0.2 / 1 / 2 detik) agar perbedaan ringan–berat terlihat di Flower. `task_acks_late=True` + `worker_prefetch_multiplier=1` di `celery_app.py:33-34` memastikan job berat tidak menumpuk di satu worker.

**Chain/group (`run_chain.py`, bonus ala contoh dosen):** `chain(hitung_nilai_akhir.s("Ani",80,75,90) | tambah_predikat.s())` menghasilkan `nilai_akhir=82.5, grade=B, lulus=true, predikat=Baik`; `group` 3 nilai paralel lalu `rekap` menghasilkan `rata2=80.1`. Jalankan tanpa broker: `python run_chain.py`; via broker: `python run_chain.py --broker`.

**Producer (`producer.py`, Tugas 3):** pembangkit data acak (`gen_mahasiswa`: nama + tugas/UTS/UAS 50–100, `--seed` agar reproduksibel). Mode `--eager` eksekusi langsung via `.apply()` (tanpa RabbitMQ, untuk demo laptop); mode default kirim via `delay` / `apply_async(exchange, routing_key)` (butuh RabbitMQ + worker, gagal dengan pesan ramah "Connection refused -> nyalakan RabbitMQ atau pakai --eager"). Contoh: `python producer.py --demo --eager` (8 pesan: 5 nilai + echo + rekap + pengumuman), `python producer.py --app nilai --num 6`, `python producer.py --app pengumuman --pesan "UAS dimajukan"`.

---

## 4. Cara Menjalankan

```bash
cd "Tugas 2/tugas2-celery"
python -m venv myenv && source myenv/bin/activate
pip install -r requirements.txt
```

Tanpa RabbitMQ (cukup untuk demo + presentasi):

```bash
python demo_routing.py
python producer.py --demo --eager
python run_chain.py
```

Dengan RabbitMQ (skenario kuliah):

```bash
# 1. Broker (pilih salah satu)
brew install rabbitmq && brew services start rabbitmq
# atau: docker run -d --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# 2. Worker (terminal terpisah)
celery -A celery_app worker --loglevel=info -Q q_echo,q_nilai -n w1@%h --concurrency=2
celery -A celery_app worker --loglevel=info -Q q_rekap,q_pengumuman -n w2@%h --concurrency=2

# 3. Flower (monitoring, buka http://localhost:5555)
celery --broker=amqp://guest:guest@localhost:5672// flower --port=5555

# 4. Producer + demo routing via broker
python producer.py --demo
python producer.py --app nilai --num 6
python demo_routing.py --broker amqp://guest:guest@localhost:5672//
python run_chain.py --broker
```

## 5. File

- `celery_app.py` — konfigurasi Celery + exchange `pengumuman` (fanout) & `kampus` (topic) + routing.
- `tasks.py` — 5 task akademik (modifikasi UTS).
- `producer.py` — Tugas 3: input generator (`--eager` / broker, fanout/topic).
- `demo_routing.py` — Tugas 1: uji Fan-Out / Topic / Wildcard.
- `run_chain.py` — bonus: `chain` + `group`.
- `hasil_demo.txt` — salinan output demo yang sudah terverifikasi.
- `requirements.txt` — `celery`, `flower`, `kombu`.
