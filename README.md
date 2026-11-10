# Tugas 2 KBJ: Job Queue & Message-Brokering (Celery & RabbitMQ)

**Mata Kuliah:** Komputasi Berbasis Jaringan (S2 Informatika, Universitas Negeri Surabaya)  
**Anggota Kelompok:**

1. Batari Wahyu Pangesti (26051905021)
2. Muhammad Umar Mansyur (26051905003)

Repository ini menggunakan **studi kasus simulasi transaksi perbankan**. Sebuah transaksi dapat menghasilkan beberapa pekerjaan, seperti validasi, transfer, pencatatan mutasi, rekap, dan notifikasi. Tugas ini menguji bagaimana pesan diarahkan ke antrean yang tepat menggunakan Kombu, kemudian bagaimana pekerjaan tersebut dikirim dan diproses menggunakan **Celery** dan **RabbitMQ**.

## 1. Pengujian Message Handling: Fan-Out, Topic, dan Wildcard

Pengujian dilakukan pada `demo_routing.py`. Studi kasusnya adalah **pendistribusian informasi transaksi perbankan** kepada layanan yang membutuhkan informasi tersebut. Perbedaan ketiga skema terletak pada **antrean mana yang menerima pesan**, bukan pada isi atau kecepatan pemrosesan transaksi.

**Fan-Out — satu informasi dikirim ke semua penerima.** Misalnya, sistem menghasilkan pesan *“Transaksi transfer berhasil”*. Informasi yang sama perlu diketahui oleh layanan pemberitahuan dan layanan pencatatan aktivitas. Pada skema *fanout*, producer cukup mengirim **satu pesan** ke *exchange*, lalu broker mengirimkan salinannya ke **antrean A dan antrean B** yang terhubung. Routing key tidak digunakan sebagai filter. Dalam konteks kasus ini, antrean A dan B dapat dianalogikan sebagai layanan notifikasi dan audit; peran tersebut merupakan ilustrasi, bukan klaim nama layanan dalam kode.

```text
Producer: "Transfer berhasil"
          |
      Fanout Exchange
          |
     +----+----+
     v         v
  Antrean A  Antrean B
  (terima)   (terima)
```

**Topic — pesan diterima berdasarkan jenis transaksi.** Misalnya, terdapat dua pesan dengan *routing key* `transaksi.transfer` dan `transaksi.validasi`. Antrean yang berlangganan `transaksi.transfer` hanya menerima pekerjaan transfer, sedangkan antrean yang berlangganan `transaksi.validasi` hanya menerima pekerjaan validasi. Dengan demikian, layanan transfer tidak perlu mengambil pesan validasi. Contoh ini menunjukkan pencocokan *routing key* secara **spesifik/eksak** pada *topic exchange*.

**Wildcard — satu antrean dapat berlangganan beberapa jenis pesan.** Pada *topic exchange*, `*` berarti **tepat satu kata**, sedangkan `#` berarti **nol atau lebih kata** yang dipisahkan tanda titik. Misalnya, layanan pemantauan ingin menerima semua kategori transaksi tanpa membuat *binding* satu per satu. Pola `transaksi.*` menerima `transaksi.transfer` dan `transaksi.validasi`, tetapi tidak menerima `transaksi.transfer.berhasil`. Sementara itu, `transaksi.#` menerima ketiganya, termasuk routing key `transaksi`.

| Pesan (routing key) | Fan-Out (A & B) | Topic `transaksi.transfer` | Wildcard `transaksi.*` | Wildcard `transaksi.#` |
| --- | :---: | :---: | :---: | :---: |
| `transaksi.transfer` | ✓ | ✓ | ✓ | ✓ |
| `transaksi.validasi` | ✓ | – | ✓ | ✓ |
| `transaksi.transfer.berhasil` | ✓ | – | – | ✓ |
| `transaksi` | ✓ | – | – | ✓ |

*Tabel menunjukkan aturan pencocokan yang diharapkan apabila semua binding di atas dikonfigurasi; tabel bukan log eksekusi.*

**Cara mencoba:**

```bash
# Demonstrasi routing lokal dengan broker memori
python demo_routing.py

# Alternatif: jalankan terhadap RabbitMQ yang sudah aktif
python demo_routing.py --broker amqp://guest:guest@localhost:5672//
```

Ringkasan demonstrasi yang dicatat pada dokumentasi proyek menunjukkan bahwa Fan-Out meneruskan satu notifikasi ke antrean A dan B, sedangkan Topic eksak memisahkan pesan transfer dari validasi. Untuk Wildcard, pesan yang diterima bergantung pada kecocokan polanya. **Kesimpulannya:** Fan-Out digunakan untuk *broadcast*, Topic eksak untuk pemisahan kategori tertentu, dan Wildcard untuk berlangganan sejumlah kategori dengan pola yang lebih fleksibel. Wildcard **bukan** jenis exchange terpisah, melainkan fasilitas pencocokan pada Topic.

## 2. Modifikasi Program UTS dengan Konsep Celery

Program UTS tentang **simulasi transaksi perbankan** dimodifikasi agar pekerjaan tidak harus dijalankan seluruhnya melalui pemanggilan fungsi secara langsung. Dengan Celery, transaksi dapat dikemas sebagai *task*, ditempatkan pada *queue* melalui broker, dan diproses oleh *worker* terpisah.

Studi kasusnya: seorang nasabah melakukan **transfer Rp500.000**. Sistem harus memvalidasi permintaan, menjalankan transfer, menghitung biaya administrasi, mencatat mutasi, dan bila diperlukan mengirim notifikasi. Pada contoh alur yang didokumentasikan, biaya administrasi **Rp2.500**, sehingga total debit menjadi **Rp502.500**. Nilai ini adalah contoh perhitungan, bukan bukti saldo rekening sungguhan telah berubah.

| Konsep Celery | Penerapan pada studi kasus |
| --- | --- |
| **Task** | Fungsi validasi, transfer, rekap, notifikasi, dan mutasi didefinisikan sebagai pekerjaan Celery di `tasks.py`. |
| **Routing / Queue** | `celery_app.py` mengatur pengiriman pekerjaan ke antrean seperti `q_validasi`, `q_transfer`, `q_rekap`, dan `q_notifikasi`. |
| **Chain** | Pekerjaan transfer diselesaikan terlebih dahulu, kemudian hasilnya diteruskan untuk pencatatan mutasi. |
| **Group** | Beberapa permintaan transfer dikirim sebagai satu kelompok tugas; pemrosesan paralel bergantung pada konfigurasi worker. |
| **Retry** | Tugas yang mengalami kegagalan tertentu dapat dicoba kembali sesuai aturan yang diterapkan. |

Contoh hubungan **Chain** pada transaksi:

```text
Permintaan transfer Rp500.000
          |
          v
     Task transfer
          |
          v
  Hasil transfer + biaya admin
          |
          v
   Task pencatatan mutasi
```

Contoh Chain dan Group disediakan pada `run_chain.py` dan dapat dicoba dengan:

```bash
python run_chain.py
```

Penggunaan Celery membuat alur pekerjaan lebih modular: pengirim tugas, antrean, dan eksekutor memiliki tanggung jawab berbeda. **Catatan:** fitur retry tidak otomatis mencegah transfer dicatat dua kali. Sistem transaksi nyata tetap memerlukan mekanisme seperti *idempotency* berdasarkan ID transaksi. Demonstrasi lokal juga belum membuktikan pengiriman melalui RabbitMQ sebelum broker dan worker dijalankan secara terpisah.

## 3. Pembuatan Producer sebagai Input Generator

`producer.py` berperan sebagai **pembangkit data transaksi**. Sebagai contoh, ketika sistem ingin menguji enam permintaan transfer, producer membuat data masukan berupa **identitas transaksi, rekening tujuan, dan nominal transfer** (contoh rentang nominal Rp50.000–Rp1.000.000). Producer kemudian mengirimkan tugas ke broker agar worker memprosesnya. Dengan demikian, producer bertugas **menghasilkan dan mengirim pekerjaan**, bukan mengeksekusi proses transfer pada mode broker normal.

```text
producer.py                     RabbitMQ                     Celery Worker
   |                               |                              |
   |-- data transaksi 1 ---------->|                              |
   |-- data transaksi 2 ---------->|--- antrean q_transfer ------>|
   |-- ...                         |                              |-- proses task
   |-- data transaksi 6 ---------->|                              |-- catat log/hasil
```

**Cara menjalankan dan mengamati alur:**

```bash
# 1. Pasang dependensi
pip install -r requirements.txt

# 2. Jalankan RabbitMQ dengan Docker
docker run -d --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# 3. Jalankan worker pada terminal terpisah
celery -A tasks:app worker --loglevel=info -Q q_validasi,q_transfer,q_rekap,q_notifikasi,celery

# 4. Kirim enam pekerjaan transfer dari terminal lain
python producer.py --app transfer --num 6
```

Keberhasilan **pengujian integrasi** diperiksa dari tiga hal: producer mengirim tugas, RabbitMQ menerima pesan ke antrean yang sesuai, dan log worker menunjukkan bahwa tugas diproses. Pengujian ini perlu dijalankan pada lingkungan dengan RabbitMQ aktif; tidak ada hasil integrasi yang diasumsikan berhasil tanpa log.

Untuk **uji fungsi lokal** tanpa broker dan worker terpisah, gunakan:

```bash
python producer.py --demo --eager
```

Pada mode `--eager`, Celery mengeksekusi fungsi langsung di proses lokal. Mode ini berguna untuk memeriksa logika tugas, tetapi **bukan** pengujian mekanisme producer–RabbitMQ–worker. Melalui ketiga bagian tugas ini, studi kasus perbankan memperlihatkan bahwa aturan *message routing* menentukan penerima pesan, Celery mengelola pekerjaan, dan producer menyediakan masukan bagi proses asynchronous.
