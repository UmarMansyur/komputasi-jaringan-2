# Tugas 2 KBJ: Job Queue & Message Brokering (Celery & RabbitMQ)

**Mata Kuliah:** Komputasi Berbasis Jaringan — S2 Informatika, Universitas Negeri Surabaya

**Anggota Kelompok:**
1. Batari Wahyu Pangesti (26051905021)
2. Muhammad Umar Mansyur (26051905003)

---

# 1. Deskripsi Proyek

Proyek ini mengimplementasikan sistem *Job Queue* (antrean pekerjaan) menggunakan Celery dan RabbitMQ sebagai *Message Broker* (perantara pesan). Sistem ini menggunakan studi kasus perbankan digital yang menggambarkan proses transfer dana antar rekening melalui aplikasi perbankan.

Nasabah dapat melakukan transfer dengan memilih rekening tujuan dan memasukkan nominal uang yang akan dikirim. Sistem memperoleh informasi rekening asal secara otomatis dari akun nasabah yang sedang digunakan. Setelah menerima permintaan transfer, sistem memeriksa saldo rekening dan memvalidasi transaksi sebelum melanjutkan proses transfer.

Proses transfer melibatkan beberapa pekerjaan, seperti pemeriksaan saldo, pemrosesan transaksi, pencatatan mutasi rekening, pengiriman notifikasi, dan pembuatan laporan. Jika seluruh pekerjaan tersebut dijalankan secara langsung dalam satu permintaan, aplikasi membutuhkan waktu lebih lama untuk memberikan respons kepada nasabah.

Untuk mengatasi masalah tersebut, proyek ini menggunakan Celery untuk mengelola pekerjaan dan RabbitMQ untuk menyimpan pesan dalam antrean. Selanjutnya, Celery Worker mengambil dan menjalankan pekerjaan tersebut secara asinkron di latar belakang. Dengan mekanisme ini, aplikasi dapat memberikan respons lebih cepat tanpa harus menunggu seluruh pekerjaan selesai.

# 2. Tugas 1 — Pengujian Message Handling (Fan-Out, Topic, dan Wildcard)

## 2.1 Konfigurasi Exchange dan Queue

Proyek ini menggunakan dua exchange utama, yaitu `bank` dengan tipe `topic` dan `notifikasi` dengan tipe `fanout`. Setiap exchange memiliki fungsi berbeda dalam mengatur distribusi pesan.

Konfigurasi exchange dan queue terdapat pada berkas `celery_app.py`, seperti berikut:

```python
broadcast = Exchange("notifikasi", type="fanout")
bank = Exchange("bank", type="topic")

Queue(
    "q_validasi",
    exchange=bank,
    routing_key="transaksi.validasi"
),

Queue(
    "q_transfer",
    exchange=bank,
    routing_key="transaksi.transfer"
),

Queue(
    "q_rekap",
    exchange=bank,
    routing_key="keuangan.rekap"
),

Queue(
    "q_notifikasi",
    exchange=broadcast
),
```

Exchange `bank` menerima pesan transaksi dan meneruskannya kepada queue berdasarkan kesesuaian routing key. Sebagai contoh, queue `q_transfer` menerima pesan dengan routing key `transaksi.transfer`, sedangkan queue `q_validasi` menerima pesan dengan routing key `transaksi.validasi`.

Sementara itu, exchange `notifikasi` menggunakan tipe `fanout` untuk mengirimkan pesan kepada seluruh queue yang terhubung dengannya. Mekanisme ini memungkinkan sistem menyebarkan satu notifikasi kepada beberapa penerima sekaligus.

Pengujian mekanisme routing terdapat pada berkas `demo_routing.py`. Program menggunakan Kombu dengan transport `memory://` sebagai media pengujian lokal sehingga pengguna dapat menjalankan demonstrasi tanpa mengaktifkan RabbitMQ.

Untuk melakukan pengujian menggunakan RabbitMQ sebenarnya, pengguna dapat menambahkan parameter berikut:

```bash
python demo_routing.py --broker amqp://guest:guest@localhost:5672//
```

Hasil pengujian tersimpan dalam berkas `hasil_demo.txt`.

## 2.2 A. Topic — Pencocokan Routing Key Secara Tepat

**Konsep**

Topic merupakan jenis exchange yang meneruskan pesan berdasarkan kecocokan antara routing key pada pesan dan binding key pada queue.

Routing key menggunakan tanda titik untuk memisahkan setiap bagian kata. Sebagai contoh, `transaksi.transfer` terdiri atas dua bagian, yaitu `transaksi` dan `transfer`.

Pada pengujian ini, program menggunakan pencocokan secara tepat. Artinya, queue hanya menerima pesan apabila routing key sesuai dengan binding key yang telah ditentukan.

**Implementasi**

Program mendefinisikan dua queue pengujian pada exchange `bank`, yaitu:

```python
Queue(
    "demo_topic_transfer",
    exchange=bank,
    routing_key="transaksi.transfer"
)

Queue(
    "demo_topic_validasi",
    exchange=bank,
    routing_key="transaksi.validasi"
)
```

Queue `demo_topic_transfer` menerima pesan dengan routing key `transaksi.transfer`, sedangkan queue `demo_topic_validasi` menerima pesan dengan routing key `transaksi.validasi`.

Selanjutnya, program mengirimkan satu pesan menggunakan routing key `transaksi.transfer`. Pada implementasi task Celery, fungsi `proses_transfer` juga menggunakan exchange `bank` dengan routing key yang sama.

**Hasil Pengujian**

Berdasarkan hasil pengujian pada `hasil_demo.txt`, program memperoleh hasil sebagai berikut:

| Queue | Jumlah Pesan | Hasil |
|---|---:|---|
| `demo_topic_transfer` | 1 | `{'id':'TRX-001','nominal':500000}` |
| `demo_topic_validasi` | 0 | Tidak menerima pesan |

Hasil tersebut menunjukkan bahwa pesan hanya diterima oleh queue yang memiliki binding key sesuai dengan routing key yang dikirimkan. Queue `demo_topic_transfer` menerima satu pesan karena kedua key memiliki nilai yang sama. Sebaliknya, queue `demo_topic_validasi` tidak menerima pesan karena menggunakan binding key yang berbeda.

Dengan demikian, pengujian ini menunjukkan bahwa topic exchange dapat memisahkan pesan berdasarkan jenis pekerjaan yang telah ditentukan.

**Penerapan**

Pada sistem perbankan digital, mekanisme ini dapat digunakan untuk memisahkan pekerjaan validasi dan transfer dana. Worker yang menangani validasi hanya menerima pekerjaan validasi, sedangkan worker yang menangani transfer hanya menerima pekerjaan transfer.

Pemisahan tersebut membantu setiap worker menjalankan tugas sesuai dengan tanggung jawabnya. Contoh lainnya adalah worker kliring yang hanya menerima pesan dengan routing key `kliring.masuk`.

## 2.3 B. Wildcard — Pencocokan Berdasarkan Pola

**Konsep**

Wildcard merupakan fitur pada topic exchange yang memungkinkan queue menerima pesan berdasarkan pola tertentu. RabbitMQ menyediakan dua simbol wildcard, yaitu `*` dan `#`.

Simbol `*` hanya cocok dengan tepat satu bagian kata. Sebagai contoh, pola `transaksi.*` dapat menerima `transaksi.transfer`, tetapi tidak menerima `transaksi.transfer.bifast`.

Sementara itu, simbol `#` dapat mencocokkan nol atau lebih bagian kata. Dengan demikian, pola `transaksi.#` dapat menerima `transaksi.transfer` maupun `transaksi.transfer.bifast`.

Pola wildcard ditentukan pada binding key milik queue, bukan pada routing key yang dikirimkan oleh producer.

**Implementasi**

Program mendefinisikan dua queue dengan pola wildcard yang berbeda pada exchange `bank`.

```python
Queue(
    "demo_wild_star",
    exchange=bank,
    routing_key="transaksi.*"
)

Queue(
    "demo_wild_hash",
    exchange=bank,
    routing_key="transaksi.#"
)
```

Queue `demo_wild_star` menggunakan pola `transaksi.*`, sedangkan queue `demo_wild_hash` menggunakan pola `transaksi.#`.

Selanjutnya, program mengirimkan tiga pesan dengan routing key berbeda, yaitu:

1. `transaksi.transfer`
2. `transaksi.transfer.bifast`
3. `kliring.masuk`

Ketiga pesan tersebut digunakan untuk menguji kemampuan masing-masing pola dalam menerima pesan.

**Hasil Pengujian**

Berdasarkan hasil pengujian pada `hasil_demo.txt`, program memperoleh hasil berikut:

| Routing Key | `transaksi.*` | `transaksi.#` |
|---|---|---|
| `transaksi.transfer` | Diterima | Diterima |
| `transaksi.transfer.bifast` | Ditolak | Diterima |
| `kliring.masuk` | Ditolak | Ditolak |
| **Jumlah pesan** | **1** | **2** |

Hasil tersebut menunjukkan bahwa pola `transaksi.*` hanya menerima satu pesan, yaitu `transaksi.transfer`. Pola tersebut menolak `transaksi.transfer.bifast` karena routing key tersebut memiliki tiga bagian kata.

Sebaliknya, pola `transaksi.#` menerima dua pesan karena simbol `#` dapat mencocokkan lebih dari satu bagian kata selama pola awalnya sesuai.

Kedua queue menolak pesan `kliring.masuk` karena pesan tersebut tidak menggunakan awalan `transaksi`.

Dengan demikian, simbol `*` membatasi pencocokan pada satu bagian kata, sedangkan simbol `#` memungkinkan pencocokan terhadap beberapa bagian kata.

**Penerapan**

Pada sistem perbankan digital, wildcard dapat digunakan untuk mengelompokkan berbagai jenis transaksi dalam satu queue.

Sebagai contoh, worker rekapitulasi dapat menggunakan pola `transaksi.#` agar menerima berbagai pesan yang berkaitan dengan transaksi, seperti `transaksi.transfer`, `transaksi.validasi`, dan `transaksi.transfer.bifast`.

Dengan cara tersebut, sistem dapat menangani beberapa jenis pekerjaan yang memiliki kategori utama sama tanpa harus membuat binding key untuk setiap jenis transaksi.

## 2.4 C. Fan-Out — Mengirim Pesan ke Semua Queue

**Konsep**

Fanout merupakan jenis exchange yang meneruskan setiap pesan kepada seluruh queue yang terhubung dengannya. Exchange ini tidak menggunakan routing key untuk menentukan queue tujuan.

Ketika producer mengirimkan satu pesan, RabbitMQ membuat salinan pesan tersebut untuk setiap queue yang terhubung. Dengan demikian, seluruh queue menerima isi pesan yang sama meskipun producer hanya mengirimkan satu pesan.

Mekanisme ini dikenal sebagai *publish/subscribe*, yaitu pola komunikasi ketika satu pengirim menyampaikan pesan kepada beberapa penerima sekaligus.

**Implementasi**

Program mendefinisikan exchange `notifikasi` dengan tipe `fanout` pada berkas `celery_app.py`.

```python
broadcast = Exchange(
    "notifikasi",
    type="fanout"
)
```

Selanjutnya, program menghubungkan queue `q_notifikasi` dengan exchange tersebut tanpa menentukan routing key.

```python
Queue(
    "q_notifikasi",
    exchange=broadcast
)
```

Task `notifikasi_transaksi` mengirimkan pesan melalui exchange `notifikasi` menggunakan metode `apply_async()`.

```python
notifikasi_transaksi.apply_async(
    args=[pesan],
    exchange="notifikasi"
)
```

Untuk menguji mekanisme distribusi pesan, program pada `demo_routing.py` membuat dua queue, yaitu `demo_fanout_A` dan `demo_fanout_B`. Kedua queue tersebut terhubung dengan exchange yang sama.

Selanjutnya, producer mengirimkan satu pesan dengan routing key `diabaikan.saja`. Pengujian ini menunjukkan bahwa fanout exchange tetap mendistribusikan pesan meskipun producer menyertakan routing key.

**Hasil Pengujian**

Berdasarkan hasil pengujian pada `hasil_demo.txt`, program memperoleh hasil berikut:

| Queue | Jumlah Pesan | Isi Pesan |
|---|---:|---|
| `demo_fanout_A` | 1 | `{'notifikasi':'Transfer Rp500.000 berhasil!'}` |
| `demo_fanout_B` | 1 | `{'notifikasi':'Transfer Rp500.000 berhasil!'}` |

Hasil tersebut menunjukkan bahwa kedua queue menerima satu pesan dengan isi yang sama. Producer hanya mengirimkan satu pesan, tetapi RabbitMQ meneruskan salinannya kepada kedua queue.

Dengan demikian, pengujian ini membuktikan bahwa fanout exchange mendistribusikan pesan kepada seluruh queue yang terhubung, bukan membagikan satu pesan hanya kepada satu penerima seperti pola pembagian pekerjaan round-robin.

**Penerapan**

Pada sistem perbankan digital, fanout dapat digunakan untuk menyebarkan informasi keberhasilan transfer kepada beberapa komponen aplikasi.

Sebagai contoh, setelah nasabah berhasil melakukan transfer, sistem dapat mengirimkan satu pesan kepada layanan notifikasi aplikasi mobile, layanan email, dan layanan pencatatan log audit.

Setiap layanan menerima salinan pesan yang sama dan menjalankan tugasnya masing-masing. Dengan demikian, producer tidak perlu mengirimkan pesan secara terpisah kepada setiap layanan.

## 2.5 Ringkasan Perbedaan Fan-Out, Topic, dan Wildcard

Ketiga mekanisme tersebut memiliki cara kerja yang berbeda dalam mendistribusikan pesan. Fanout mengirimkan pesan kepada seluruh queue yang terhubung, topic mencocokkan routing key dengan binding key, sedangkan wildcard menggunakan pola tertentu untuk menentukan pesan yang dapat diterima.

| Aspek | Fan-Out | Topic | Wildcard |
|---|---|---|---|
| Tipe exchange | `fanout` (`notifikasi`) | `topic` (`bank`) | `topic` (`bank`) dengan pola |
| Peran routing key | Diabaikan | Dicocokkan secara tepat | Dicocokkan berdasarkan pola |
| Penerima pesan | Semua queue yang terhubung | Queue dengan binding yang cocok | Queue dengan pola yang cocok |
| Contoh binding | Tanpa key khusus | `transaksi.transfer` | `transaksi.*` / `transaksi.#` |
| Analogi | Pengeras suara bank | Pesan untuk bagian tertentu | Kelompok penerima berdasarkan kategori |

# 3. Tugas 2 — Modifikasi Program UTS dengan Celery

Proyek ini mengembangkan program UTS yang sebelumnya menggunakan socket menjadi sistem antrean pekerjaan menggunakan Celery dan RabbitMQ. Pada implementasi ini, setiap pekerjaan didaftarkan sebagai task Celery dengan awalan nama `tugas2.*`.

Setiap task menggunakan konfigurasi `bind=True` dan `max_retries=3`. Konfigurasi `bind=True` memungkinkan task mengakses informasi internal tugas melalui parameter `self`. Sementara itu, `max_retries=3` menentukan batas maksimal percobaan ulang apabila program menjalankan mekanisme retry.

Program juga menggunakan fungsi `sleep()` dengan durasi 0,2 detik, 1 detik, dan 2 detik untuk menyimulasikan perbedaan waktu pemrosesan pekerjaan ringan, sedang, dan berat. Perbedaan tersebut dapat diamati melalui Flower.

Selain itu, konfigurasi `task_acks_late=True` membuat worker memberikan pengakuan kepada RabbitMQ setelah tugas selesai diproses. Konfigurasi `worker_prefetch_multiplier=1` membatasi jumlah pesan yang diambil sebelumnya oleh worker. Kedua pengaturan tersebut membantu mengatur distribusi pekerjaan ketika beberapa tugas memiliki waktu pemrosesan yang berbeda.

## 3.1 Chain — Menjalankan Task Secara Berurutan

Chain merupakan mekanisme Celery yang menjalankan beberapa task secara berurutan. Hasil dari task pertama akan diteruskan sebagai masukan bagi task berikutnya.

Pada proyek ini, chain menghubungkan proses transfer dana dengan proses pencatatan mutasi rekening. Implementasinya terdapat pada berkas `run_chain.py`.

```python
chain(
    proses_transfer.s(
        "8812340001",
        "8856780501",
        500000
    )
    | kirim_mutasi.s()
)
```

Pada contoh tersebut, task `proses_transfer` menerima rekening asal, rekening tujuan, dan nominal transfer sebesar Rp500.000. Setelah proses transfer selesai, Celery meneruskan hasilnya kepada task `kirim_mutasi`.

Task `kirim_mutasi` kemudian mencatat informasi transaksi dan menghasilkan identitas mutasi rekening.

Berdasarkan hasil demonstrasi, program menghasilkan keluaran berikut:

```text
total_debet = 502500.0
status = sukses+mutasi
mutasi_id = MUT-8812340001-500000
```

Hasil tersebut menunjukkan bahwa kedua task berhasil dijalankan secara berurutan. Proses pencatatan mutasi menggunakan hasil dari proses transfer sebelumnya.

Dengan demikian, chain dapat digunakan untuk menghubungkan beberapa pekerjaan yang saling membutuhkan hasil dari proses sebelumnya.

## 3.2 Group — Menjalankan Task Secara Paralel

Group merupakan mekanisme Celery yang memungkinkan beberapa task dijalankan secara paralel. Setiap task dapat diproses secara terpisah selama tidak bergantung pada hasil task lainnya.

Pada proyek ini, group menjalankan tiga proses transfer dengan nominal berbeda, yaitu Rp500.000, Rp250.000, dan Rp1.000.000. Setelah seluruh proses selesai, program mengumpulkan hasilnya dan menjalankan proses rekapitulasi.

Berdasarkan hasil demonstrasi, program menghasilkan informasi berikut:

```text
jumlah = 3
total = 1757500.0
rata2 = 585833.33
```

Hasil tersebut menunjukkan bahwa sistem berhasil menjalankan tiga task secara paralel dan menggabungkan hasilnya dalam proses rekapitulasi.

Dengan demikian, group dapat digunakan ketika sistem harus menjalankan beberapa pekerjaan yang tidak saling bergantung sebelum menggabungkan hasilnya.

## 3.3 Menjalankan Chain dan Group

Pengguna dapat menjalankan demonstrasi chain dan group menggunakan dua mode berikut:

```bash
# Menjalankan secara langsung tanpa RabbitMQ
python run_chain.py

# Menjalankan melalui RabbitMQ
python run_chain.py --broker
```

Mode pertama menjalankan task secara langsung tanpa menggunakan RabbitMQ. Sementara itu, mode kedua mengirimkan task melalui RabbitMQ sehingga membutuhkan broker dan worker yang aktif.

# 4. Tugas 3 — Producer sebagai Input Generator

Berkas `producer.py` berfungsi menghasilkan data transaksi secara otomatis dan mengirimkan pekerjaan kepada Celery.

Program menggunakan fungsi `gen_transaksi` untuk membangkitkan data transaksi yang terdiri atas identitas transaksi, nomor rekening, dan nominal transfer. Program menghasilkan identitas transaksi seperti `TRX-001`, nomor rekening dengan format `88...`, serta nominal transfer secara acak dalam rentang Rp50.000 sampai Rp1.000.000.

Producer juga menyediakan parameter `--seed` untuk menentukan nilai awal pembangkit bilangan acak. Parameter tersebut memungkinkan program menghasilkan data yang sama ketika pengujian dijalankan kembali dengan nilai seed yang sama.

Program menyediakan dua mode pengiriman pekerjaan, yaitu mode eager dan mode broker.

## 4.1 Mode Eager

Mode eager menjalankan task secara langsung menggunakan metode `.apply()`. Pada mode ini, producer tidak mengirimkan pesan kepada RabbitMQ sehingga pengguna tidak perlu mengaktifkan broker maupun worker.

Mode ini cocok untuk menguji fungsi program dan menjalankan demonstrasi sederhana pada komputer lokal.

## 4.2 Mode Broker

Mode broker mengirimkan task kepada RabbitMQ menggunakan metode `delay()` atau `apply_async()`. RabbitMQ menerima pesan tersebut dan meneruskannya kepada queue yang sesuai. Selanjutnya, worker mengambil pesan dan menjalankan pekerjaan yang diterima.

Mode ini membutuhkan RabbitMQ dan Celery Worker yang aktif. Apabila koneksi ke RabbitMQ gagal, program akan menampilkan pesan `Connection refused` dan meminta pengguna mengaktifkan RabbitMQ atau menggunakan mode `--eager`.

Perbedaan kedua mode tersebut dapat dilihat pada tabel berikut.

| Aspek | Mode Eager | Mode Broker |
|---|---|---|
| Metode | `.apply()` | `.delay()` / `.apply_async()` |
| RabbitMQ | Tidak diperlukan | Diperlukan |
| Worker | Tidak diperlukan | Diperlukan |
| Cara kerja | Task dijalankan langsung | Task dikirim melalui antrean |
| Tujuan | Pengujian fungsi dan demonstrasi | Pengujian distribusi pekerjaan |

## 4.3 Contoh Penggunaan Producer

Pengguna dapat menjalankan producer menggunakan beberapa perintah berikut.

```bash
# Menjalankan demonstrasi dengan delapan pesan
# 5 transfer + 1 validasi + 1 rekap + 1 notifikasi
python producer.py --demo --eager

# Menghasilkan enam transaksi transfer
python producer.py --app transfer --num 6

# Mengirimkan notifikasi transaksi
python producer.py --app notifikasi --pesan "Transfer Rp500.000 berhasil!"
```

Perintah pertama menjalankan seluruh demonstrasi menggunakan mode eager. Perintah kedua menghasilkan enam pekerjaan transfer, sedangkan perintah ketiga mengirimkan pesan notifikasi melalui aplikasi Celery.

# 5. Cara Menjalankan Program

## 5.1 Persiapan Lingkungan

Sebelum menjalankan program, pengguna perlu membuat lingkungan virtual Python dan memasang seluruh pustaka yang dibutuhkan.

```bash
# Membuat lingkungan virtual
python -m venv myenv

# Mengaktifkan lingkungan virtual
source myenv/bin/activate

# Memasang pustaka yang dibutuhkan
pip install -r requirements.txt
```

Proyek ini menggunakan Celery untuk mengelola task dan worker, Flower untuk memantau aktivitas worker, serta Kombu untuk menangani komunikasi antara Celery dan broker.

## 5.2 Menjalankan Tanpa RabbitMQ

Pengguna dapat menjalankan demonstrasi lokal tanpa mengaktifkan RabbitMQ menggunakan perintah berikut.

```bash
# Menguji mekanisme routing
python demo_routing.py

# Menjalankan producer dalam mode eager
python producer.py --demo --eager

# Menjalankan demonstrasi chain dan group
python run_chain.py
```

Pengujian tersebut digunakan untuk melihat mekanisme routing, menjalankan task secara langsung, serta menguji hubungan antartugas.

Pada pengujian routing, program menggunakan `memory://` sehingga pesan diproses melalui media penyimpanan sementara di dalam memori. Pengujian ini tidak menggunakan server RabbitMQ sebenarnya.

## 5.3 Menjalankan dengan RabbitMQ

Untuk menguji komunikasi menggunakan RabbitMQ sebenarnya, pengguna perlu menjalankan broker, worker, dan producer secara terpisah.

**A. Menjalankan RabbitMQ**

Pengguna macOS dapat menjalankan RabbitMQ melalui Homebrew.

```bash
brew services start rabbitmq
```

Sebagai alternatif, pengguna dapat menjalankan RabbitMQ menggunakan Docker.

```bash
docker run -d \
  --name rabbit \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management
```

**B. Menjalankan Celery Worker**

Buka terminal pertama untuk menjalankan worker yang menangani validasi dan transfer.

```bash
celery -A celery_app worker \
  --loglevel=info \
  -Q q_validasi,q_transfer \
  -n w1@%h \
  --concurrency=2
```

Selanjutnya, buka terminal kedua untuk menjalankan worker yang menangani rekapitulasi dan notifikasi.

```bash
celery -A celery_app worker \
  --loglevel=info \
  -Q q_rekap,q_notifikasi \
  -n w2@%h \
  --concurrency=2
```

Pembagian queue tersebut membuat setiap worker menangani kelompok pekerjaan yang berbeda. Parameter `--concurrency=2` memungkinkan setiap worker menjalankan maksimal dua proses pekerjaan secara bersamaan.

**C. Menjalankan Flower**

Buka terminal ketiga untuk menjalankan Flower.

```bash
celery \
  --broker=amqp://guest:guest@localhost:5672// \
  flower \
  --port=5555
```

Setelah Flower berjalan, pengguna dapat membuka alamat berikut melalui peramban:

http://localhost:5555

Halaman Flower menampilkan informasi aktivitas worker dan pelaksanaan task sehingga pengguna dapat memantau pekerjaan yang sedang berjalan maupun yang telah selesai.

**D. Menjalankan Producer dan Pengujian Routing**

Setelah RabbitMQ dan worker aktif, buka terminal keempat untuk menjalankan producer.

```bash
# Menjalankan demonstrasi seluruh task
python producer.py --demo

# Menghasilkan enam transaksi transfer
python producer.py --app transfer --num 6

# Mengirimkan notifikasi
python producer.py --app notifikasi --pesan "Transfer Rp500.000 berhasil!"

# Menguji routing menggunakan RabbitMQ
python demo_routing.py --broker amqp://guest:guest@localhost:5672//

# Menjalankan chain dan group melalui RabbitMQ
python run_chain.py --broker
```

Perintah tersebut digunakan untuk menguji pengiriman pesan, distribusi pekerjaan kepada queue, pemrosesan oleh worker, serta pemantauan melalui Flower.

# 6. Struktur dan Fungsi Berkas

Proyek ini terdiri atas beberapa berkas yang memiliki fungsi berbeda. Setiap berkas mendukung proses pengelolaan task, pengiriman pesan, pengujian routing, dan pemantauan hasil pekerjaan.

| Nama Berkas | Fungsi |
|---|---|
| `celery_app.py` | Mengatur konfigurasi Celery, exchange `bank` bertipe topic, exchange `notifikasi` bertipe fanout, serta routing dan queue. |
| `tasks.py` | Mendefinisikan lima task perbankan yang digunakan dalam sistem. |
| `producer.py` | Menghasilkan data transaksi dan mengirimkan task melalui mode eager atau RabbitMQ. |
| `demo_routing.py` | Menguji mekanisme distribusi pesan menggunakan fanout, topic, dan wildcard. |
| `run_chain.py` | Menjalankan demonstrasi chain dan group. |
| `hasil_demo.txt` | Menyimpan keluaran hasil demonstrasi yang digunakan sebagai dokumentasi pengujian. |
| `requirements.txt` | Menyimpan daftar pustaka Python yang diperlukan, yaitu Celery, Flower, dan Kombu. |

# 7. Kesimpulan

Berdasarkan implementasi dan hasil pengujian, proyek ini memperlihatkan penerapan Job Queue menggunakan Celery dan RabbitMQ pada studi kasus perbankan digital.

Pada Tugas 1, pengujian memperlihatkan tiga mekanisme distribusi pesan. Topic exchange meneruskan pesan berdasarkan kecocokan routing key, wildcard menggunakan pola `*` dan `#` untuk menentukan pesan yang diterima, sedangkan fanout exchange mengirimkan salinan pesan kepada seluruh queue yang terhubung.

Pada Tugas 2, proyek ini mengembangkan program UTS yang sebelumnya menggunakan socket menjadi sistem antrean pekerjaan menggunakan Celery dan RabbitMQ. Implementasi chain menghubungkan proses transfer dengan pencatatan mutasi, sedangkan group menjalankan beberapa proses transfer secara paralel sebelum menggabungkan hasilnya dalam rekapitulasi.

Pada Tugas 3, producer menghasilkan data transaksi secara otomatis dengan nominal acak yang dapat dibuat kembali menggunakan parameter `--seed`. Producer dapat menjalankan pekerjaan secara langsung melalui mode eager atau mengirimkannya melalui RabbitMQ untuk diproses oleh worker.

Secara keseluruhan, proyek ini memperlihatkan bagaimana Celery mengelola task dan worker, RabbitMQ mendistribusikan pesan melalui exchange dan queue, serta Kombu menangani komunikasi antara Celery dan broker.

Pengujian menggunakan `memory://` hanya menunjukkan mekanisme routing melalui transport memori, sedangkan pengujian menggunakan RabbitMQ diperlukan untuk membuktikan komunikasi dengan broker sebenarnya. Seluruh hasil pengujian yang dilaporkan dalam dokumentasi ini mengacu pada keluaran program yang tercatat dalam `hasil_demo.txt`.