# Tugas 2 KBJ: Job Queue & Message Brokering (Celery & RabbitMQ)

**Komputasi Berbasis Jaringan — S2 Informatika, Universitas Negeri Surabaya**
1. Batari Wahyu Pangesti (26051905021)
2. Muhammad Umar Mansyur (26051905003)

## 1. Deskripsi Proyek

Implementasi *job queue* dengan **Celery + RabbitMQ** pada studi kasus **Sistem Akademik Sederhana**: dosen input nilai 1 kelas (40 mhs) lalu tekan "Hitung & Rekap" — hitung nilai akhir per mhs, rekap statistik kelas, dan sebar pengumuman. Jika dikerjakan sinkron di 1 request web, respons lambat. Solusi: request dilempar ke antrean lalu dikerjakan *worker* secara **asinkron** di belakang layar.

Alur AMQP (4 tahap):

1. **Producer → Exchange** (`producer.py` kirim ke `kampus` / `pengumuman`, tanpa tunjuk worker langsung).
2. **Exchange → Queue** (aturan routing: `topic` pakai routing key, `fanout` salin ke semua queue).
3. **Queue menyimpan pesan** (`q_nilai`, `q_rekap`, dsb.) sampai worker ambil.
4. **Worker proses + ack** (`task_acks_late=True`, `max_retries=3`, `prefetch_multiplier=1` agar job berat tidak menumpuk).

5 task di `tasks.py` (`NA = 0.3*Tugas + 0.3*UTS + 0.4*UAS`):

| Task | Beban | Fungsi |
|---|---|---|
| `echo_nama` | ringan (SHORT UTS) | `"Ani"` → `"Halo, Ani!"` |
| `hitung_nilai_akhir` | sedang | hitung NA + grade A–E |
| `rekap_kelas` | berat (LONG UTS) | jumlah, rata-rata, min, maks, distribusi grade |
| `broadcast_pengumuman` | siaran | 1 pesan → semua queue via `fanout` |
| `tambah_predikat` | sambungan `chain` | terima output NA → tambah `lulus` + `predikat` |

UTS (socket mentah) → Tugas 2:

| UTS | Tugas 2 | Perubahan |
|---|---|---|
| `broker.py` TCP:5000 + round-robin/least-requests | RabbitMQ + `task_routes`/`task_queues` di `celery_app.py` | seleksi worker manual → routing AMQP (`-Q` + `--concurrency`) |
| `worker.py` SHORT/LONG | `tasks.py` echo/hitung/rekap | pola sama, domain jadi akademik |
| `client.py` JSON + `request_id` via TCP | `producer.py` via `delay`/`apply_async` + `task_id` | framing manual → Celery + `rpc://` backend |

## 2. Message Handling

Konfigurasi (`celery_app.py`):

```python
broadcast = Exchange("pengumuman", type="fanout")
kampus = Exchange("kampus", type="topic")
Queue("q_echo", exchange=kampus, routing_key="teks.echo"),
Queue("q_nilai", exchange=kampus, routing_key="nilai.akhir"),
Queue("q_rekap", exchange=kampus, routing_key="nilai.rekap"),
Queue("q_pengumuman", exchange=broadcast),
```

Uji murni AMQP: `python demo_routing.py` (`memory://`, tanpa server; tambah `--broker amqp://...` untuk RabbitMQ asli). Hasil di `hasil_demo.txt`.

| Jenis | Aturan | Hasil demo |
|---|---|---|
| **Topic** (`kampus`, binding eksak `nilai.tugas` vs `nilai.uts`, kirim `nilai.tugas`) | hanya queue yang key-nya sama persis menerima | `nilai.tugas`: **1** (`{'mhs':'Ani','nilai':80}`), `nilai.uts`: **0** → cocok untuk tugas spesifik per worker |
| **Wildcard** (`nilai.*` vs `nilai.#`, kirim `nilai.tugas`, `nilai.tugas.remidi`, `absensi.masuk`) | `*`=tepat 1 kata, `#`=0..N kata | `nilai.*`: **1** (hanya `nilai.tugas`); `nilai.#`: **2** (`tugas` + `remidi`); keduanya tolak `absensi` → cocok untuk agregasi (`nilai.#`) |
| **Fan-out** (`pengumuman`, 2 queue, kirim 1 pesan, key diabaikan) | salin ke **semua** queue ter-bind | A: **1**, B: **1**, isi identik (`Kuliah daring besok!`) → cocok untuk pengumuman massal; beda dengan round-robin UTS (1 request → 1 worker) |

## 3. Modifikasi UTS + Producer

* **Task** (`tasks.py`, `bind=True`, `max_retries=3`, `sleep` 0.2/1/2 dtk agar terlihat di Flower).
* **Chain/Group** (`run_chain.py`): `chain(hitung("Ani",80,75,90) | predikat)` → `NA=82.5, B, lulus, Baik`; `group` 3 nilai paralel → rekap `rata2=80.1`. Jalankan: `python run_chain.py` / `python run_chain.py --broker`.
* **Producer** (`producer.py`, `gen_mahasiswa` acak 50–100 + `--seed`): `--eager` = `.apply()` langsung tanpa broker (demo laptop); default = `delay`/`apply_async(exchange, routing_key)` butuh RabbitMQ+worker. Contoh: `python producer.py --demo --eager` (8 pesan), `python producer.py --app nilai --num 6`, `python producer.py --app pengumuman --pesan "UAS dimajukan"`.

## 4. Cara Menjalankan

```bash
python -m venv myenv && source myenv/bin/activate
pip install -r requirements.txt  # celery, flower, kombu
# Tanpa RabbitMQ:
python demo_routing.py
python producer.py --demo --eager
python run_chain.py
```

```bash
# Dengan RabbitMQ:
brew services start rabbitmq  # atau: docker run -d --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management
# Terminal 1-2 (worker):
celery -A celery_app worker --loglevel=info -Q q_echo,q_nilai -n w1@%h --concurrency=2
celery -A celery_app worker --loglevel=info -Q q_rekap,q_pengumuman -n w2@%h --concurrency=2
# Terminal 3 (monitoring http://localhost:5555):
celery --broker=amqp://guest:guest@localhost:5672// flower --port=5555
# Terminal 4:
python producer.py --demo
python demo_routing.py --broker amqp://guest:guest@localhost:5672//
python run_chain.py --broker
```

## 5. File

| File | Isi |
|---|---|
| `celery_app.py` | config Celery + exchange `kampus` (topic) & `pengumuman` (fanout) + routing |
| `tasks.py` | 5 task akademik (modifikasi UTS) |
| `producer.py` | input generator (`--eager`/broker) |
| `demo_routing.py` | uji Fan-Out/Topic/Wildcard |
| `run_chain.py` | demo `chain` + `group` |
| `hasil_demo.txt` | output demo terverifikasi |
| `requirements.txt` | `celery`, `flower`, `kombu` |

## 6. Kesimpulan

Producer kirim ke exchange tanpa tunggu selesai → RabbitMQ teruskan ke queue via routing key/binding → worker ambil sesuai `-Q` dan ack setelah selesai. Terbukti: topic memisah pesan eksak, `*` menyempitkan (1 level) sedangkan `#` memperluas (semua turunan), fan-out menyalin 1 pesan ke semua queue. Chain/group memungkinkan alur berurutan maupun paralel. Arsitektur ini menggantikan broker socket manual UTS menjadi message brokering yang terstruktur dan asinkron.
