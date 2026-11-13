"""DB 팀 샘플 데이터를 파싱해 환자별 요약을 출력한다.

사용: python3 scripts/summarize_samples.py
"""
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from fhir_parser import parse_file  # noqa: E402
from specialty_map import load_encounter_info, specialty_label  # noqa: E402

DATA = ROOT / "sample_data" / "carecompass_6patients"
info = load_encounter_info(DATA / "query_models" / "encounter_timeline.json")
result = parse_file(DATA / "fhir" / "all-patients.bundle.json", encounter_info=info)

print(f"Records {len(result.records)}건 | FollowUps {len(result.followups)}건 | 원문(S3) {len(result.resources)}건 | 오류 {len(result.errors)}건")
print("리소스 종류:", dict(Counter(r["resourceType"] for r in result.records)))

for patient in sorted({r["patientKey"] for r in result.records}):
    visits = sorted((r for r in result.records if r["patientKey"] == patient and r["resourceType"] == "Encounter"),
                    key=lambda r: r["occurredAt"] or "")
    specialties = Counter(specialty_label(r["specialtyCode"]) for r in visits)
    print(f"\n=== {patient}: 진료 {len(visits)}회 {dict(specialties)}")
    dated = [v['occurredAt'] for v in visits if v['occurredAt']]
    print(f"    {dated[0][:10]} ~ {dated[-1][:10]}" if dated else "    날짜 없음")
    for f in (f for f in result.followups if f["patientKey"] == patient):
        print(f"    후속: {f['kind']:<11} {f['sourceStatus']:<10} {f['scheduledAt'] or f['scheduledDate'] or '날짜 미정'}  {f['title']}")
