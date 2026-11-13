"""적재 점검 (dry run): 실제로 보내지 않고, 환자별로 무엇을 S3·DB에 보낼지만 보여 준다. AWS 호출 없음.

실행: python3 scripts/ingest_dry_run.py [환자ID ...]     (기본: 샘플 6명 전부)
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from fhir_validate import ValidationReport  # noqa: E402
from ingest import ingest_patient  # noqa: E402
from specialty_map import load_encounter_info  # noqa: E402

DATA = ROOT / "sample_data" / "carecompass_6patients"


class DryRunOnly:
    """dry run 전용. 검증을 생략했다고 표시만 한다(dry run은 검증 뒤 단계로 넘어가지 않으므로 attestation이 생기지 않음)."""

    def validate(self, bundle_bytes, name):
        return ValidationReport(True, "dry-run(검증 생략)")


def main(patients: list[str]) -> None:
    info = load_encounter_info(DATA / "query_models" / "encounter_timeline.json")
    for pid in patients:
        bundle = (DATA / "fhir" / "bundles" / f"{pid}.bundle.json").read_bytes()
        report = ingest_patient(bundle, fhir_patient_id=pid, patient_key=pid, validator=DryRunOnly(),
                                store=None, writer=None, encounter_info=info, dry_run=True)
        s = report.summary()
        print(f"{pid}: Records {s['records']} / FollowUps {s['followups']} / S3 원문 {s['s3Objects']} / "
              f"요청 {s['batches']}회 {report.batches} / 변환 제외 {s['parserErrors']}건")


if __name__ == "__main__":
    main(sys.argv[1:] or ["A1", "A2", "B1", "B2", "C1", "C2"])
