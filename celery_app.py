"""Konfigurasi Celery — Tugas 2 KBJ (demo perbankan, mudah).

Studi kasus : Sistem Perbankan Digital (transfer + validasi + mutasi + notifikasi + rekap).
Broker default: amqp://guest:guest@localhost:5672// (RabbitMQ, sesuai contoh dosen).
Tanpa RabbitMQ pun kode tetap bisa didemo via mode eager / memory://
(lihat producer.py --eager dan demo_routing.py).

Pemetaan dari program UTS:
  UTS broker.py (socket, round_robin)  -> Celery broker (RabbitMQ) + task_routes
  UTS worker.py (SHORT echo, LONG prima/faktorial) -> tasks.py (validasi, transfer, rekap)
  UTS client.py (kirim JSON via TCP)   -> producer.py (input generator via delay/apply_async)
"""

import os
from celery import Celery
from kombu import Exchange, Queue

BROKER_URL = os.getenv("CELERY_BROKER_URL", "amqp://guest:guest@localhost:5672//")
BACKEND_URL = os.getenv("CELERY_RESULT_BACKEND", "rpc://")

# Exchange untuk Tugas 1 (message handling)
broadcast = Exchange("notifikasi", type="fanout")  # Fan-Out: 1 pesan -> semua queue
bank = Exchange("bank", type="topic")               # Topic + Wildcard

app = Celery("tugas2_bank", broker=BROKER_URL, backend=BACKEND_URL)

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
        "tugas2.validasi_transaksi": {
            "queue": "q_validasi",
            "exchange": "bank",
            "routing_key": "transaksi.validasi",
        },
        "tugas2.proses_transfer": {
            "queue": "q_transfer",
            "exchange": "bank",
            "routing_key": "transaksi.transfer",
        },
        "tugas2.rekap_keuangan": {
            "queue": "q_rekap",
            "exchange": "bank",
            "routing_key": "keuangan.rekap",
        },
        "tugas2.notifikasi_transaksi": {
            "queue": "q_notifikasi",
            "exchange": "notifikasi",
        },
    },
    task_queues=(
        Queue("q_validasi", exchange=bank, routing_key="transaksi.validasi"),
        Queue("q_transfer", exchange=bank, routing_key="transaksi.transfer"),
        Queue("q_rekap", exchange=bank, routing_key="keuangan.rekap"),
        Queue("q_notifikasi", exchange=broadcast),
    ),
)
