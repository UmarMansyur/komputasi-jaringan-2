"""Konfigurasi Celery — Tugas 2 KBJ (demo akademik, mudah).

Studi kasus : Sistem Akademik Sederhana (pengumuman + pengolahan nilai).
Broker default: amqp://guest:guest@localhost:5672// (RabbitMQ, sesuai contoh dosen).
Tanpa RabbitMQ pun kode tetap bisa didemo via mode eager / memory://
(lihat producer.py --eager dan demo_routing.py).

Pemetaan dari program UTS:
  UTS broker.py (socket, round_robin)  -> Celery broker (RabbitMQ) + task_routes
  UTS worker.py (SHORT echo, LONG prima/faktorial) -> tasks.py (echo_nama, hitung_nilai_akhir, rekap_kelas)
  UTS client.py (kirim JSON via TCP)   -> producer.py (input generator via delay/apply_async)
"""

import os
from celery import Celery
from kombu import Exchange, Queue

BROKER_URL = os.getenv("CELERY_BROKER_URL", "amqp://guest:guest@localhost:5672//")
BACKEND_URL = os.getenv("CELERY_RESULT_BACKEND", "rpc://")

# Exchange untuk Tugas 1 (message handling)
broadcast = Exchange("pengumuman", type="fanout")   # Fan-Out: 1 pesan -> semua queue
kampus = Exchange("kampus", type="topic")           # Topic + Wildcard

app = Celery("tugas2_akademik", broker=BROKER_URL, backend=BACKEND_URL)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Asia/Jakarta",
    enable_utc=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    # Routing default tiap task (bisa dioverride saat apply_async)
    task_routes={
        "tugas2.echo_nama": {
            "queue": "q_echo",
            "exchange": "kampus",
            "routing_key": "teks.echo",
        },
        "tugas2.hitung_nilai_akhir": {
            "queue": "q_nilai",
            "exchange": "kampus",
            "routing_key": "nilai.akhir",
        },
        "tugas2.rekap_kelas": {
            "queue": "q_rekap",
            "exchange": "kampus",
            "routing_key": "nilai.rekap",
        },
        "tugas2.broadcast_pengumuman": {
            "queue": "q_pengumuman",
            "exchange": "pengumuman",
        },
    },
    task_queues=(
        Queue("q_echo", exchange=kampus, routing_key="teks.echo"),
        Queue("q_nilai", exchange=kampus, routing_key="nilai.akhir"),
        Queue("q_rekap", exchange=kampus, routing_key="nilai.rekap"),
        Queue("q_pengumuman", exchange=broadcast),
    ),
)
