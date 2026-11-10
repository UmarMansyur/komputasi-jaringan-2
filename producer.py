"""Producer perbankan: bangkitkan transaksi acak (--eager tanpa broker)."""

import argparse
import json
import random
import time

AKUN_LOGIN = "8800001234"


def gen_transaksi(rng: random.Random, i: int) -> dict:
    tujuan = f"88{rng.randint(1000, 9999)}{i + 500:04d}"
    return {
        "id": f"TRX-{i:03d}",
        "asal": AKUN_LOGIN,
        "tujuan": tujuan,
        "nominal": rng.randint(50000, 1000000),
        "saldo": 2000000,
    }


def send_eager(app_name: str, jobs: list):
    from tasks import validasi_transaksi, proses_transfer, rekap_keuangan, notifikasi_transaksi

    out = []
    for job in jobs:
        kind = job["kind"]
        if kind == "validasi":
            d = job["data"]
            r = validasi_transaksi.apply(args=[d["id"], d["asal"], d["tujuan"], d["nominal"], d.get("saldo", 2000000)]).get()
        elif kind == "transfer":
            d = job["data"]
            r = proses_transfer.apply(args=[d["asal"], d["tujuan"], d["nominal"]]).get()
        elif kind == "rekap":
            r = rekap_keuangan.apply(args=[job["daftar"]]).get()
        elif kind == "notifikasi":
            r = notifikasi_transaksi.apply(args=[job["pesan"]]).get()
        else:
            r = {"error": f"kind {kind}?"}
        print(f"[PRODUCER-eager] {kind} -> {json.dumps(r)}")
        out.append(r)
    return out


def send_broker(app_name: str, jobs: list):
    from tasks import validasi_transaksi, proses_transfer, rekap_keuangan, notifikasi_transaksi

    out = []
    try:
        for job in jobs:
            kind = job["kind"]
            if kind == "validasi":
                d = job["data"]
                r = validasi_transaksi.apply_async(
                    args=[d["id"], d["asal"], d["tujuan"], d["nominal"], d.get("saldo", 2000000)],
                    exchange="bank", routing_key="transaksi.validasi",
                )
            elif kind == "transfer":
                d = job["data"]
                r = proses_transfer.apply_async(
                    args=[d["asal"], d["tujuan"], d["nominal"]],
                    exchange="bank", routing_key="transaksi.transfer",
                )
            elif kind == "rekap":
                r = rekap_keuangan.apply_async(
                    args=[job["daftar"]], exchange="bank", routing_key="keuangan.rekap"
                )
            elif kind == "notifikasi":
                r = notifikasi_transaksi.apply_async(
                    args=[job["pesan"]], exchange="notifikasi"
                )
            else:
                continue
            print(f"[PRODUCER] terkirim {kind} task_id={r.id} (menunggu worker...)")
            out.append(r)
    except Exception as exc:  # noqa: BLE001
        print("[PRODUCER] GAGAL kirim ke broker:", exc)
        print("[PRODUCER] Pastikan RabbitMQ jalan atau ulangi dengan --eager.")
        return []
    for r in out:
        try:
            print(f"[PRODUCER] hasil {r.id}: {json.dumps(r.get(timeout=30))}")
        except Exception as exc:  # noqa: BLE001
            print(f"[PRODUCER] hasil {r.id} belum tersedia: {exc}")
    return out


def main():
    ap = argparse.ArgumentParser(description="Producer input generator (perbankan demo)")
    ap.add_argument("--app", default="campuran",
                    help="validasi | transfer | rekap | notifikasi | campuran")
    ap.add_argument("--num", type=int, default=5, help="jumlah pesan yang dibangkitkan")
    ap.add_argument("--pesan", default="Transfer Rp500.000 berhasil!", help="isi notifikasi")
    ap.add_argument("--demo", action="store_true",
                    help="kirim contoh campuran: validasi+transfer+rekap+notifikasi")
    ap.add_argument("--eager", action="store_true",
                    help="eksekusi langsung tanpa broker/RabbitMQ")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    jobs = []
    if args.demo or args.app == "campuran":
        for i in range(max(1, args.num)):
            d = gen_transaksi(rng, i + 1)
            jobs.append({"kind": "transfer", "data": d})
        jobs.append({"kind": "validasi", "data": gen_transaksi(rng, 99)})
        jobs.append({"kind": "rekap", "daftar": [150000, 75000, 500000, 120000, 300000]})
        jobs.append({"kind": "notifikasi", "pesan": args.pesan})
    elif args.app == "validasi":
        for i in range(args.num):
            jobs.append({"kind": "validasi", "data": gen_transaksi(rng, i + 1)})
    elif args.app == "transfer":
        for i in range(args.num):
            jobs.append({"kind": "transfer", "data": gen_transaksi(rng, i + 1)})
    elif args.app == "rekap":
        for _ in range(args.num):
            jobs.append({"kind": "rekap",
                         "daftar": [rng.randint(50000, 1000000) for _ in range(10)]})
    elif args.app == "notifikasi":
        for _ in range(args.num):
            jobs.append({"kind": "notifikasi", "pesan": args.pesan})
    else:
        raise SystemExit(f"--app tidak dikenal: {args.app}")

    print(f"[PRODUCER] membangkitkan {len(jobs)} pesan (mode={'eager' if args.eager else 'broker'})")
    t0 = time.time()
    if args.eager:
        send_eager(args.app, jobs)
    else:
        send_broker(args.app, jobs)
    print(f"[PRODUCER] selesai dalam {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
