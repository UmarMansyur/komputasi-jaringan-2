"""Producer — pembangkit data masukan (input generator) untuk broker + worker.

Tugas 3: skrip producer yang pesannya diterima broker dan dikerjakan worker.
Domain akademik (demo saja): data mahasiswa + nilai acak.

Mode:
  --eager   : tanpa RabbitMQ, task dieksekusi langsung (untuk demo di laptop).
  default   : kirim ke broker (butuh RabbitMQ + worker jalan, lihat README).

Contoh:
    python producer.py --demo --eager            # demo cepat tanpa broker
    python producer.py --demo                    # kirim 5 job campuran ke broker
    python producer.py --app nilai --num 6       # 6 nilai acak ke topic nilai.akhir
    python producer.py --app pengumuman --pesan "UAS dimajukan" --num 1
    python producer.py --app rekap --num 1 --eager
"""

import argparse
import json
import random
import time

NAMES = ["Ani", "Budi", "Citra", "Dimas", "Eka", "Fajar", "Gita", "Hadi"]


def gen_mahasiswa(rng: random.Random, i: int) -> dict:
    return {
        "nama": f"{rng.choice(NAMES)}-{i:03d}",
        "tugas": rng.randint(50, 100),
        "uts": rng.randint(50, 100),
        "uas": rng.randint(50, 100),
    }


def send_eager(app_name: str, jobs: list):
    """Jalankan task langsung tanpa broker (Celery eager)."""
    from tasks import echo_nama, hitung_nilai_akhir, rekap_kelas, broadcast_pengumuman

    out = []
    for job in jobs:
        kind = job["kind"]
        if kind == "echo":
            r = echo_nama.apply(args=[job["nama"]]).get()
        elif kind == "nilai":
            d = job["data"]
            r = hitung_nilai_akhir.apply(args=[d["nama"], d["tugas"], d["uts"], d["uas"]]).get()
        elif kind == "rekap":
            r = rekap_kelas.apply(args=[job["daftar"]]).get()
        elif kind == "pengumuman":
            r = broadcast_pengumuman.apply(args=[job["pesan"]]).get()
        else:
            r = {"error": f"kind {kind}?"}
        print(f"[PRODUCER-eager] {kind} -> {json.dumps(r)}")
        out.append(r)
    return out


def send_broker(app_name: str, jobs: list):
    """Kirim task ke broker RabbitMQ (butuh worker hidup)."""
    from tasks import echo_nama, hitung_nilai_akhir, rekap_kelas, broadcast_pengumuman

    out = []
    try:
        for job in jobs:
            kind = job["kind"]
            if kind == "echo":
                r = echo_nama.delay(job["nama"])
            elif kind == "nilai":
                d = job["data"]
                # routing topic eksplisit (Tugas 1): nilai.akhir
                r = hitung_nilai_akhir.apply_async(
                    args=[d["nama"], d["tugas"], d["uts"], d["uas"]],
                    exchange="kampus", routing_key="nilai.akhir",
                )
            elif kind == "rekap":
                r = rekap_kelas.apply_async(
                    args=[job["daftar"]], exchange="kampus", routing_key="nilai.rekap"
                )
            elif kind == "pengumuman":
                # fan-out: exchange pengumuman, routing_key diabaikan
                r = broadcast_pengumuman.apply_async(
                    args=[job["pesan"]], exchange="pengumuman"
                )
            else:
                continue
            print(f"[PRODUCER] terkirim {kind} task_id={r.id} (menunggu worker...)")
            out.append(r)
    except Exception as exc:  # noqa: BLE001 - mis. RabbitMQ belum jalan
        print("[PRODUCER] GAGAL kirim ke broker:", exc)
        print("[PRODUCER] Pastikan RabbitMQ jalan (brew services start rabbitmq / docker)")
        print("[PRODUCER] atau ulangi dengan mode --eager (tanpa broker).")
        return []
    # tunggu hasil (opsional, butuh result backend)
    for r in out:
        try:
            print(f"[PRODUCER] hasil {r.id}: {json.dumps(r.get(timeout=30))}")
        except Exception as exc:  # noqa: BLE001
            print(f"[PRODUCER] hasil {r.id} belum tersedia: {exc}")
    return out


def main():
    ap = argparse.ArgumentParser(description="Producer input generator (akademik demo)")
    ap.add_argument("--app", default="campuran",
                    help="echo | nilai | rekap | pengumuman | campuran")
    ap.add_argument("--num", type=int, default=5, help="jumlah pesan yang dibangkitkan")
    ap.add_argument("--pesan", default="Kuliah daring besok!", help="isi pengumuman")
    ap.add_argument("--demo", action="store_true",
                    help="kirim contoh campuran: echo+nilai+rekap+pengumuman")
    ap.add_argument("--eager", action="store_true",
                    help="eksekusi langsung tanpa broker/RabbitMQ")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    jobs = []
    if args.demo or args.app == "campuran":
        for i in range(max(1, args.num)):
            d = gen_mahasiswa(rng, i + 1)
            jobs.append({"kind": "nilai", "data": d})
        jobs.append({"kind": "echo", "nama": "Ani"})
        jobs.append({"kind": "rekap", "daftar": [70, 85, 90, 55, 78]})
        jobs.append({"kind": "pengumuman", "pesan": args.pesan})
    elif args.app == "echo":
        for i in range(args.num):
            jobs.append({"kind": "echo", "nama": gen_mahasiswa(rng, i + 1)["nama"]})
    elif args.app == "nilai":
        for i in range(args.num):
            jobs.append({"kind": "nilai", "data": gen_mahasiswa(rng, i + 1)})
    elif args.app == "rekap":
        for _ in range(args.num):
            jobs.append({"kind": "rekap",
                         "daftar": [rng.randint(50, 100) for _ in range(10)]})
    elif args.app == "pengumuman":
        for _ in range(args.num):
            jobs.append({"kind": "pengumuman", "pesan": args.pesan})
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
