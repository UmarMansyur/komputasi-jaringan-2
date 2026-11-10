"""Demo chain/group Celery perbankan (eager default, --broker untuk RabbitMQ)."""

import argparse
import json


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--broker", action="store_true", help="lewat broker, bukan eager")
    args = ap.parse_args()

    from celery import chain, group
    from celery_app import app
    from tasks import proses_transfer, kirim_mutasi, rekap_keuangan

    if not args.broker:
        app.conf.task_always_eager = True

    print("=== Chain: proses_transfer -> kirim_mutasi ===")
    job = chain(
        proses_transfer.s("8812340001", "8856780501", 500000),
        kirim_mutasi.s(),
    )
    res = job.apply_async()
    print(json.dumps(res.get(timeout=30), indent=2))

    print("\n=== Group: 3 transfer paralel + rekap ===")
    g = group(
        proses_transfer.s("8812340001", "8856780501", 500000),
        proses_transfer.s("8811110002", "8822220502", 250000),
        proses_transfer.s("8833330003", "8844440503", 1000000),
    )
    hasil = g.apply_async().get(timeout=30)
    print(json.dumps(hasil, indent=2))
    total_list = [h["total_debet"] for h in hasil]
    print("rekap:", json.dumps(rekap_keuangan.apply_async(args=[total_list]).get(timeout=30), indent=2))


if __name__ == "__main__":
    main()
