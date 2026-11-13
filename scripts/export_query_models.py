"""6명·36회 샘플을 schemaVersion 2 조회 모델로 변환해 파일로 저장한다 (팀 전달·검토용).

실행: python3 scripts/export_query_models.py  →  output/v2/
  Records.jsonl     upsert_records.records 에 들어갈 행 (105)
  FollowUps.jsonl   upsert_records.followups 에 들어갈 행
  s3_objects.json   {s3Key: sourceSha256}  — S3에 올릴 원문 목록 (바이트는 fhir_parser.serialize_resource)
  upsert_batches/<환자>.json  환자별 upsert 요청 params 예시 (expectedStateVersion·sourceVerification 제외)
  errors.json       변환 실패 건
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from fhir_parser import parse_file, sha256_hex, upsert_batches  # noqa: E402
from specialty_map import load_encounter_info  # noqa: E402

DATA = ROOT / "sample_data" / "carecompass_6patients"
OUT = ROOT / "output" / "v2"


def dump_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n" for r in rows), encoding="utf-8")


def main() -> None:
    info = load_encounter_info(DATA / "query_models" / "encounter_timeline.json")
    result = parse_file(DATA / "fhir" / "all-patients.bundle.json", encounter_info=info)
    (OUT / "upsert_batches").mkdir(parents=True, exist_ok=True)
    dump_jsonl(OUT / "Records.jsonl", sorted(result.records, key=lambda r: r["resourceKey"]))
    dump_jsonl(OUT / "FollowUps.jsonl", result.followups)
    objects = {key: sha256_hex(data) for key, data in sorted(result.object_bytes().items())}
    (OUT / "s3_objects.json").write_text(json.dumps(objects, indent=2) + "\n", encoding="utf-8")
    (OUT / "errors.json").write_text(json.dumps(result.errors, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    for patient in result.patients():
        batches = upsert_batches(result.records, result.followups, patient)
        (OUT / "upsert_batches" / f"{patient}.json").write_text(
            json.dumps(batches, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Records {len(result.records)} / FollowUps {len(result.followups)} / S3 {len(objects)} / 오류 {len(result.errors)}"
          f" → {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
