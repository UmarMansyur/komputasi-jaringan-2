"""Demo message handling: Fan-Out vs Topic vs Wildcard (domain akademik).

Hanya DEMO routing AMQP memakai kombu, default memory:// sehingga bisa jalan
TANPA RabbitMQ di laptop mana pun. Dengan --broker amqp://... bisa dites
ke RabbitMQ asli.

Contoh:
    python demo_routing.py
    python demo_routing.py --broker amqp://guest:guest@localhost:5672//
"""

import argparse
from kombu import Connection, Exchange, Queue


def _drain(channel, queue: Queue):
    """Ambil semua pesan yang menunggu di queue (lalu ack)."""
    out = []
    q = queue(channel)
    while True:
        msg = q.get()
        if msg is None:
            break
        out.append(msg.payload)
        msg.ack()
    return out


def demo_fanout(conn: Connection):
    print("=== 1. Fan-Out (exchange 'pengumuman', type fanout) ===")
    print("Contoh akademik: 1 pengumuman 'Kuliah daring besok' harus diterima SEMUA prodi.")
    print("Aturan: routing_key DIABAIKAN, 1 pesan disalin ke semua queue yang ter-bind.")
    ch = conn.channel()
    ex = Exchange("pengumuman", type="fanout")
    qa = Queue("demo_fanout_A", exchange=ex, routing_key="")
    qb = Queue("demo_fanout_B", exchange=ex, routing_key="")
    qa(ch).declare()
    qb(ch).declare()
    # bersihkan sisa lama
    _drain(ch, qa)
    _drain(ch, qb)
    conn.Producer(ch).publish(
        {"pengumuman": "Kuliah daring besok!"}, exchange=ex, routing_key="diabaikan.saja"
    )
    ra, rb = _drain(ch, qa), _drain(ch, qb)
    print(f"  kirim 1 pesan  -> queue A terima {len(ra)}, queue B terima {len(rb)}")
    print(f"  isi A: {ra}")
    print(f"  isi B: {rb}")
    print("  Kesimpulan: KEDUA queue dapat SALINAN yang sama (broadcast).\n")
    return {"A": ra, "B": rb}


def demo_topic(conn: Connection):
    print("=== 2. Topic (exchange 'kampus', type topic, routing EKS AK) ===")
    print("Contoh akademik: nilai.tugas / nilai.uts / nilai.uas masing-masing ke worker-nya.")
    print("Aturan: pesan hanya masuk queue yang binding-nya SAMA PERSIS dengan routing_key.")
    ch = conn.channel()
    ex = Exchange("kampus", type="topic")
    qt = Queue("demo_topic_tugas", exchange=ex, routing_key="nilai.tugas")
    qu = Queue("demo_topic_uts", exchange=ex, routing_key="nilai.uts")
    qt(ch).declare()
    qu(ch).declare()
    _drain(ch, qt)
    _drain(ch, qu)
    prod = conn.Producer(ch)
    prod.publish({"mhs": "Ani", "nilai": 80}, exchange=ex, routing_key="nilai.tugas")
    rt, ru = _drain(ch, qt), _drain(ch, qu)
    print("  kirim routing_key='nilai.tugas'")
    print(f"  -> queue 'nilai.tugas' terima {len(rt)} pesan: {rt}")
    print(f"  -> queue 'nilai.uts'   terima {len(ru)} pesan: {ru}")
    print("  Kesimpulan: hanya queue yang cocok EKS AK yang menerima.\n")
    return {"tugas": rt, "uts": ru}


def demo_wildcard(conn: Connection):
    print("=== 3. Wildcard (masih exchange topic, binding pakai * dan #) ===")
    print("Contoh akademik: 'nilai.*' dan 'nilai.#' untuk rekap fleksibel.")
    print("Aturan: '*' = tepat 1 kata, '#' = 0 kata atau lebih (dipisah titik).")
    ch = conn.channel()
    ex = Exchange("kampus", type="topic")
    q_star = Queue("demo_wild_star", exchange=ex, routing_key="nilai.*")
    q_hash = Queue("demo_wild_hash", exchange=ex, routing_key="nilai.#")
    q_star(ch).declare()
    q_hash(ch).declare()
    _drain(ch, q_star)
    _drain(ch, q_hash)
    prod = conn.Producer(ch)
    prod.publish({"n": 1}, exchange=ex, routing_key="nilai.tugas")      # 2 level
    prod.publish({"n": 2}, exchange=ex, routing_key="nilai.tugas.remidi")  # 3 level
    prod.publish({"n": 3}, exchange=ex, routing_key="absensi.masuk")    # tidak terkait nilai
    rs, rh = _drain(ch, q_star), _drain(ch, q_hash)
    print("  kirim 3 pesan: 'nilai.tugas', 'nilai.tugas.remidi', 'absensi.masuk'")
    print(f"  -> binding 'nilai.*' terima {len(rs)} pesan: {rs}")
    print("     ('*' hanya cocok 2-level: nilai.tugas YA, nilai.tugas.remidi TIDAK)")
    print(f"  -> binding 'nilai.#' terima {len(rh)} pesan: {rh}")
    print("     ('#' cocok semua awalan nilai.: 2-level YA, 3-level YA, absensi TIDAK)")
    print("  Kesimpulan: wildcard memperluas/menyempitkan cakupan topic tanpa tambah exchange.\n")
    return {"star": rs, "hash": rh}


def main():
    ap = argparse.ArgumentParser(description="Demo Fan-Out / Topic / Wildcard")
    ap.add_argument("--broker", default="memory://",
                    help="memory:// (default, tanpa server) atau amqp://guest:guest@localhost:5672//")
    args = ap.parse_args()

    print(f"[broker] {args.broker}\n")
    conn = Connection(args.broker)
    conn.ensure_connection(max_retries=1)
    demo_fanout(conn)
    demo_topic(conn)
    demo_wildcard(conn)
    print("--- Ringkasan perbedaan ---")
    print("Fan-Out  : 1 pesan -> SEMUA queue (routing_key diabaikan). Contoh: pengumuman kampus.")
    print("Topic    : 1 pesan -> queue dengan routing_key SAMA PERSIS. Contoh: nilai.tugas vs nilai.uts.")
    print("Wildcard : pola di atas topic; '*' = 1 kata, '#' = 0..N kata. Contoh: nilai.* vs nilai.#.")


if __name__ == "__main__":
    main()
