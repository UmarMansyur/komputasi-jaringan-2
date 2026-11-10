"""Tasks Celery — modifikasi program UTS ke Celery (domain akademik, demo saja).

Analogi UTS -> Celery:
  SHORT (echo cepat)          -> echo_nama            : cepat, tanpa hitung berat
  LONG  (prima / faktorial)   -> hitung_nilai_akhir   : sedang (1 mahasiswa)
                              -> rekap_kelas          : berat (1 kelas, loop banyak)
  broadcast (tambahan T2)     -> broadcast_pengumuman : fan-out ke semua worker
"""

import time
from celery_app import app


def _grade(na: float) -> str:
    if na >= 85:
        return "A"
    if na >= 70:
        return "B"
    if na >= 60:
        return "C"
    if na >= 50:
        return "D"
    return "E"


@app.task(name="tugas2.echo_nama", bind=True, max_retries=3)
def echo_nama(self, nama: str):
    """Task ringan (analogi SHORT): echo / sapa. Masuk queue q_echo."""
    try:
        nama = (nama or "").strip() or "Mahasiswa"
        time.sleep(0.2)  # simulasi kerja ringan
        return {"nama": nama, "sapa": f"Halo, {nama}!", "status": "ok"}
    except Exception as exc:  # noqa: BLE001
        raise self.retry(countdown=5, exc=exc)


@app.task(name="tugas2.hitung_nilai_akhir", bind=True, max_retries=3)
def hitung_nilai_akhir(self, nama: str, tugas: float, uts: float, uas: float):
    """Task sedang: Nilai Akhir = 30% tugas + 30% UTS + 40% UAS. Queue q_nilai."""
    try:
        time.sleep(1)  # simulasi kerja sedang
        na = round(0.3 * float(tugas) + 0.3 * float(uts) + 0.4 * float(uas), 2)
        return {
            "nama": nama,
            "tugas": tugas,
            "uts": uts,
            "uas": uas,
            "nilai_akhir": na,
            "grade": _grade(na),
            "status": "ok",
        }
    except Exception as exc:  # noqa: BLE001
        raise self.retry(countdown=5, exc=exc)


@app.task(name="tugas2.rekap_kelas", bind=True, max_retries=3)
def rekap_kelas(self, daftar_nilai: list):
    """Task berat (analogi LONG): rekap 1 kelas. Queue q_rekap."""
    try:
        time.sleep(2)  # simulasi kerja berat
        vals = [float(x) for x in (daftar_nilai or [])]
        if not vals:
            return {"jumlah": 0, "status": "kosong"}
        grades = [_grade(v) for v in vals]
        dist = {g: grades.count(g) for g in sorted(set(grades))}
        return {
            "jumlah": len(vals),
            "rata2": round(sum(vals) / len(vals), 2),
            "min": min(vals),
            "max": max(vals),
            "distribusi_grade": dist,
            "status": "ok",
        }
    except Exception as exc:  # noqa: BLE001
        raise self.retry(countdown=5, exc=exc)


@app.task(name="tugas2.broadcast_pengumuman", bind=True)
def broadcast_pengumuman(self, pesan: str):
    """Task fan-out: 1 pesan diterima SEMUA worker yang bind ke exchange pengumuman."""
    time.sleep(0.2)
    return {"pengumuman": pesan, "status": "disiarkan"}


@app.task(name="tugas2.tambah_predikat", bind=True)
def tambah_predikat(self, hasil_nilai: dict):
    """Sambungan chain (demo): terima dict dari hitung_nilai_akhir, tambah lulus/predikat."""
    na = float(hasil_nilai.get("nilai_akhir", 0))
    hasil_nilai["lulus"] = na >= 60
    hasil_nilai["predikat"] = "Dengan Pujian" if na >= 85 else ("Baik" if na >= 70 else "Cukup")
    hasil_nilai["status"] = "ok+predikat"
    return hasil_nilai
