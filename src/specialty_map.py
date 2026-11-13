"""진료과 정보 (DB 팀 데이터 규칙에 맞춤).

진료과는 FHIR 원본에서 추정하지 않는다. DB 팀 패키지는 시나리오에서 정한 진료과를
병원이 신고한 건강보험심사평가원(HIRA) 진료과 코드로 연결해 `query_models/encounter_timeline.json`에 담았고,
Records의 `specialtySource`는 'DEMO_MAPPING'이다. 파서는 이 파일을 읽어 그대로 쓴다.

- Encounter.class(외래·응급 등)는 진료과가 아니다.
- 매핑 정보가 없으면 specialtyCode='UNKNOWN', specialtySource='UNKNOWN'이다.
- 실제 병원 연동 단계에서는 진료과를 어디서 가져올지(병원 API 신고값 등) 팀이 따로 정해야 한다.
"""
from __future__ import annotations

import json
from pathlib import Path

UNKNOWN = "UNKNOWN"

# 샘플 데이터에 나오는 HIRA 진료과 코드만 담은 표시명 표. 새 진료과가 생기면 여기에 추가한다.
SPECIALTY_LABELS_KO = {
    "01": "내과",
    "05": "정형외과",
    "14": "피부과",
    UNKNOWN: "확인 불가",
}


def specialty_label(code: str | None) -> str:
    """화면 표시명. 표에 없는 코드는 '확인 불가'로 돌려준다(코드를 지어내지 않는다)."""
    return SPECIALTY_LABELS_KO.get(code or UNKNOWN, SPECIALTY_LABELS_KO[UNKNOWN])


def load_encounter_info(path: str | Path) -> dict[str, dict]:
    """encounter_timeline.json → {'Encounter/A1-01': {...}}

    specialtyCode/specialtySource/specialtyEvidence(근거 문장)/specialtyName: 진료과 매핑과 근거
    clinicKey/clinicName: 병원 매핑 (FHIR Organization에 없을 때만 사용)
    summary: 진료 제목 (Encounter title)
    """
    with open(path, encoding="utf-8-sig") as fh:
        rows = json.load(fh)
    return {
        row["resource"]: {
            "specialtyCode": row.get("specialtyCode", UNKNOWN),
            "specialtySource": row.get("specialtySource", UNKNOWN),
            "specialtyEvidence": row.get("specialtyEvidence"),
            "specialtyName": row.get("specialtyName"),
            "clinicKey": row.get("clinicKey"),
            "clinicName": row.get("clinicName"),
            "summary": row.get("summary"),
        }
        for row in rows
    }
