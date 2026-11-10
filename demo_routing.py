"""Demo routing AMQP: Fan-Out vs Topic vs Wildcard (perbankan, memory://)."""

import argparse
from kombu import Connection, Exchange, Queue


def _drain(channel, queue: Queue):
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
    print("=== 1. Fan-Out (exchange 'notifikasi', type fanout) ===")
    print("Contoh bank: 1 notifikasi 'Transfer berhasil' harus diterima SEMUA kanal (mobile + email).")
    print("Aturan: routing_key DIABAIKAN, 1 pesan disalin ke semua queue yang ter-bind.")
    ch = conn.channel()
    ex = Exchange("notifikasi", type="fanout")
    qa = Queue("demo_fanout_A", exchange=ex, routing_key="")
    qb = Queue("demo_fanout_B", exchange=ex, routing_key="")
    qa(ch).declare()
    qb(ch).declare()
    _drain(ch, qa)
    _drain(ch, qb)
    conn.Producer(ch).publish(
        {"notifikasi": "Transfer Rp500.000 berhasil!"}, exchange=ex, routing_key="diabaikan.saja"
    )
    ra, rb = _drain(ch, qa), _drain(ch, qb)
    print(f"  kirim 1 pesan  -> queue A terima {len(ra)}, queue B terima {len(rb)}")
    print(f"  isi A: {ra}")
    print(f"  isi B: {rb}")
    print("  Kesimpulan: KEDUA queue dapat SALINAN yang sama (broadcast).\n")
    return {"A": ra, "B": rb}


def demo_topic(conn: Connection):
    print("=== 2. Topic (exchange 'bank', type topic, routing EKSAK) ===")
    print("Contoh bank: transaksi.transfer / transaksi.validasi masing-masing ke worker-nya.")
    print("Aturan: pesan hanya masuk queue yang binding-nya SAMA PERSIS dengan routing_key.")
    ch = conn.channel()
    ex = Exchange("bank", type="topic")
    qt = Queue("demo_topic_transfer", exchange=ex, routing_key="transaksi.transfer")
    qv = Queue("demo_topic_validasi", exchange=ex, routing_key="transaksi.validasi")
    qt(ch).declare()
    qv(ch).declare()
    _drain(ch, qt)
    _drain(ch, qv)
    prod = conn.Producer(ch)
    prod.publish({"id": "TRX-001", "nominal": 500000}, exchange=ex, routing_key="transaksi.transfer")
    rt, rv = _drain(ch, qt), _drain(ch, qv)
    print("  kirim routing_key='transaksi.transfer'")
    print(f"  -> queue 'transaksi.transfer' terima {len(rt)} pesan: {rt}")
    print(f"  -> queue 'transaksi.validasi' terima {len(rv)} pesan: {rv}")
    print("  Kesimpulan: hanya queue yang cocok EKSAK yang menerima.\n")
    return {"transfer": rt, "validasi": rv}


def demo_wildcard(conn: Connection):
    print("=== 3. Wildcard (masih exchange topic, binding pakai * dan #) ===")
    print("Contoh bank: 'transaksi.*' dan 'transaksi.#' untuk rekap fleksibel.")
    print("Aturan: '*' = tepat 1 kata, '#' = 0 kata atau lebih (dipisah titik).")
    ch = conn.channel()
    ex = Exchange("bank", type="topic")
    q_star = Queue("demo_wild_star", exchange=ex, routing_key="transaksi.*")
    q_hash = Queue("demo_wild_hash", exchange=ex, routing_key="transaksi.#")
    q_star(ch).declare()
    q_hash(ch).declare()
    _drain(ch, q_star)
    _drain(ch, q_hash)
    prod = conn.Producer(ch)
    prod.publish({"n": 1}, exchange=ex, routing_key="transaksi.transfer")
    prod.publish({"n": 2}, exchange=ex, routing_key="transaksi.transfer.bifast")
    prod.publish({"n": 3}, exchange=ex, routing_key="kliring.masuk")
    rs, rh = _drain(ch, q_star), _drain(ch, q_hash)
    print("  kirim 3 pesan: 'transaksi.transfer', 'transaksi.transfer.bifast', 'kliring.masuk'")
    print(f"  -> binding 'transaksi.*' terima {len(rs)} pesan: {rs}")
    print("     ('*' hanya cocok 2-level: transaksi.transfer YA, .bifast TIDAK)")
    print(f"  -> binding 'transaksi.#' terima {len(rh)} pesan: {rh}")
    print("     ('#' cocok semua awalan transaksi.: 2-level YA, 3-level YA, kliring TIDAK)")
    print("  Kesimpulan: wildcard memperluas/menyempitkan cakupan topic tanpa tambah exchange.\n")
    return {"star": rs, "hash": rh}


def main():
    ap = argparse.ArgumentParser(description="Demo Fan-Out / Topic / Wildcard")
    ap.add_argument("--broker", default="memory://",
                    help="memory:// (default) atau amqp://guest:guest@localhost:5672//")
    args = ap.parse_args()

    print(f"[broker] {args.broker}\n")
    conn = Connection(args.broker)
    conn.ensure_connection(max_retries=1)
    demo_fanout(conn)
    demo_topic(conn)
    demo_wildcard(conn)
    print("--- Ringkasan perbedaan ---")
    print("Fan-Out  : 1 pesan -> SEMUA queue (routing_key diabaikan). Contoh: notifikasi bank.")
    print("Topic    : 1 pesan -> queue dengan routing_key SAMA PERSIS. Contoh: transaksi.transfer vs transaksi.validasi.")
    print("Wildcard : pola di atas topic; '*' = 1 kata, '#' = 0..N kata. Contoh: transaksi.* vs transaksi.#.")


if __name__ == "__main__":
    main()
