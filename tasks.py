"""Tasks Celery — modifikasi program UTS ke Celery (domain perbankan, demo saja).

Analogi UTS -> Celery:
  SHORT (echo cepat)          -> validasi_transaksi   : cepat, cek sederhana
  LONG  (prima / faktorial)   -> proses_transfer      : sedang (1 transaksi)
                               -> rekap_keuangan       : berat (banyak transaksi, loop)
  broadcast (tambahan T2)     -> notifikasi_transaksi : fan-out ke semua kanal
"""

import time
from celery_app import app

BIAYA_ADMIN = 2500


def _kategori(nominal: float) -> str:
    if nominal < 100000:
        return "kecil"
    if nominal <= 500000:
        return "sedang"
    return "besar"


@app.task(name="tugas2.validasi_transaksi", bind=True, max_retries=3)
def validasi_transaksi(self, id_transaksi: str, asal: str, tujuan: str, nominal: float, saldo: float = 2000000):
    """Task ringan (analogi SHORT): validasi + cek saldo. Masuk queue q_validasi."""
    try:
        time.sleep(0.2)  # simulasi kerja ringan
        nominal = float(nominal)
        saldo = float(saldo)
        if nominal <= 0:
            alasan = "nominal harus positif"
        elif asal == tujuan:
            alasan = "rekening asal dan tujuan sama"
        elif nominal > 10000000:
            alasan = "nominal melebihi limit Rp10.000.000"
        elif nominal + BIAYA_ADMIN > saldo:
            alasan = "saldo tidak mencukupi"
        else:
            alasan = "ok"
        return {
            "id_transaksi": id_transaksi,
            "asal": asal,
            "tujuan": tujuan,
            "nominal": nominal,
            "saldo": saldo,
            "valid": alasan == "ok",
            "alasan": alasan,
            "status": "ok",
        }
    except Exception as exc:  # noqa: BLE001
        raise self.retry(countdown=5, exc=exc)


@app.task(name="tugas2.proses_transfer", bind=True, max_retries=3)
def proses_transfer(self, asal: str, tujuan: str, nominal: float):
    """Task sedang: proses 1 transfer + biaya admin. Queue q_transfer."""
    try:
        time.sleep(1)  # simulasi kerja sedang
        nominal = float(nominal)
        total_debet = round(nominal + BIAYA_ADMIN, 2)
        return {
            "asal": asal,
            "tujuan": tujuan,
            "nominal": nominal,
            "biaya_admin": BIAYA_ADMIN,
            "total_debet": total_debet,
            "status": "sukses",
        }
    except Exception as exc:  # noqa: BLE001
        raise self.retry(countdown=5, exc=exc)


@app.task(name="tugas2.rekap_keuangan", bind=True, max_retries=3)
def rekap_keuangan(self, daftar_nominal: list):
    """Task berat (analogi LONG): rekap banyak transaksi. Queue q_rekap."""
    try:
        time.sleep(2)  # simulasi kerja berat
        vals = [float(x) for x in (daftar_nominal or [])]
        if not vals:
            return {"jumlah": 0, "status": "kosong"}
        kats = [_kategori(v) for v in vals]
        dist = {k: kats.count(k) for k in sorted(set(kats))}
        return {
            "jumlah": len(vals),
            "total_nominal": round(sum(vals), 2),
            "rata2": round(sum(vals) / len(vals), 2),
            "min": min(vals),
            "max": max(vals),
            "distribusi_kategori": dist,
            "status": "ok",
        }
    except Exception as exc:  # noqa: BLE001
        raise self.retry(countdown=5, exc=exc)


@app.task(name="tugas2.notifikasi_transaksi", bind=True)
def notifikasi_transaksi(self, pesan: str):
    """Task fan-out: 1 pesan diterima SEMUA kanal yang bind ke exchange notifikasi."""
    time.sleep(0.2)
    return {"notifikasi": pesan, "status": "terkirim"}


@app.task(name="tugas2.kirim_mutasi", bind=True)
def kirim_mutasi(self, hasil_transfer: dict):
    """Sambungan chain (demo): terima dict dari proses_transfer, tambah mutasi."""
    hasil_transfer["mutasi_id"] = f"MUT-{hasil_transfer.get('asal', '000')}-{int(float(hasil_transfer.get('nominal', 0)))}"
    hasil_transfer["mutasi"] = "tercatat"
    hasil_transfer["status"] = "sukses+mutasi"
    return hasil_transfer
