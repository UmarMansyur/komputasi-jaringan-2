"""Tasks Celery perbankan: validasi, transfer, rekap, notifikasi, mutasi."""

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
    """Validasi ringan (SHORT): cek nominal, rekening, limit, saldo."""
    try:
        time.sleep(0.2)
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
    """Transfer sedang: total_debet = nominal + admin."""
    try:
        time.sleep(1)
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
    """Rekap berat (LONG): jumlah, total, rata2, min, maks, kategori."""
    try:
        time.sleep(2)
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
    """Fan-out: satu pesan ke semua kanal."""
    time.sleep(0.2)
    return {"notifikasi": pesan, "status": "terkirim"}


@app.task(name="tugas2.kirim_mutasi", bind=True)
def kirim_mutasi(self, hasil_transfer: dict):
    """Chain: terima hasil transfer, tambah mutasi_id."""
    hasil_transfer["mutasi_id"] = f"MUT-{hasil_transfer.get('asal', '000')}-{int(float(hasil_transfer.get('nominal', 0)))}"
    hasil_transfer["mutasi"] = "tercatat"
    hasil_transfer["status"] = "sukses+mutasi"
    return hasil_transfer
