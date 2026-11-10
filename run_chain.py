"""Demo chain / group Celery (analogi run_chain.py contoh dosen, versi akademik).

Tanpa broker: pakai eager sehingga bisa jalan di laptop mana pun.
Dengan broker: hapus baris task_always_eager dan jalankan worker dulu.

Contoh:
    python run_chain.py          # eager, tanpa RabbitMQ
    python run_chain.py --broker # via broker (butuh RabbitMQ + worker)
"""

import argparse
import json


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--broker", action="store_true", help="lewat broker, bukan eager")
    args = ap.parse_args()

    from celery import chain, group
    from celery_app import app
    from tasks import hitung_nilai_akhir, tambah_predikat, rekap_kelas

    if not args.broker:
        app.conf.task_always_eager = True  # demo tanpa RabbitMQ

    print("=== Chain: hitung_nilai_akhir -> tambah_predikat ===")
    job = chain(
        hitung_nilai_akhir.s("Ani", 80, 75, 90),
        tambah_predikat.s(),
    )
    res = job.apply_async()
    print(json.dumps(res.get(timeout=30), indent=2))

    print("\n=== Group: 3 nilai paralel + rekap ===")
    g = group(
        hitung_nilai_akhir.s("Ani", 80, 75, 90),
        hitung_nilai_akhir.s("Budi", 60, 65, 70),
        hitung_nilai_akhir.s("Citra", 90, 95, 92),
    )
    hasil = g.apply_async().get(timeout=30)
    print(json.dumps(hasil, indent=2))
    na_list = [h["nilai_akhir"] for h in hasil]
    print("rekap:", json.dumps(rekap_kelas.apply_async(args=[na_list]).get(timeout=30), indent=2))


if __name__ == "__main__":
    main()
