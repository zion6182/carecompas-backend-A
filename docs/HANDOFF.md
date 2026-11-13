# CareCompass FHIR 파서 v2 — 전달 문서

> **참고 (저장소 공개 시 추가):** 이 문서는 2026-11-06에 팀에 전달한 파서 패키지의 원본 문서입니다.
> 이후 저장소에는 S3 저장·DB 적재 코드(`src/ingest.py` 등)와 테스트가 추가되었으므로, 10·11절의 "적재 코드 없음"은 현재 저장소 기준과 다릅니다.
> DB Lambda 호출 방식은 2026-11-06 DB 팀 공지에 따라 `apiVersion "2"`(Cognito access token)로 바뀌었고, 데이터 행 형식(schemaVersion 2)은 그대로입니다.


작성: 2026-11-06 (같은 날 갱신: 환자 간 혼입·누락 방지, 배포 백엔드 파서와 환자 판별 규칙 통일)

---

## 1. 최신 파서 코드

| 파일 | 내용 |
|---|---|
| `src/fhir_parser.py` | 파서 본체. `parse_bundle()` / `parse_file()` / `upsert_batches()` / `date_model()` / `serialize_resource()` |
| `src/specialty_map.py` | `encounter_timeline.json`(진료과·병원·제목 매핑) 읽기, 진료과 표시명 |

- 출력: DB 쓰기 Lambda `upsert_records`의 `params.records` / `params.followups`에 그대로 넣을 수 있는 **schemaVersion 2** 행
- 파서는 **표준 라이브러리만** 사용합니다. AWS SDK가 필요 없습니다.
- 조회 API 코드는 별도 구현이 있어 **이 패키지에 넣지 않았습니다.**

## 2. 실행 방법

```bash
cd carecompass_fhir_parser_v2_2026-11-06
python3 -m pip install -r requirements-dev.txt   # pytest (테스트용)
python3 -m pytest -q                             # 테스트 67개
python3 scripts/export_query_models.py           # 6명 샘플 → output/v2/
python3 scripts/summarize_samples.py             # 환자별 요약 출력
```

코드에서 쓰는 방법:

```python
import sys; sys.path.insert(0, "src")
from fhir_parser import parse_file, upsert_batches
from specialty_map import load_encounter_info

DATA = "sample_data/carecompass_6patients"
info = load_encounter_info(f"{DATA}/query_models/encounter_timeline.json")
result = parse_file(f"{DATA}/fhir/all-patients.bundle.json", encounter_info=info)

result.records          # list[dict]  Records 행
result.followups        # list[dict]  FollowUps 행
result.object_bytes()   # {s3Key: bytes}  S3에 올릴 원문 (sourceSha256과 일치하는 바이트)
result.errors           # [{resource, error}]  변환 실패 건
upsert_batches(result.records, result.followups, "A1")  # [{records, followups}] 요청당 50행 이하
```

## 3. 실행 환경·라이브러리 버전·설정 예시

원문 직렬화(`serialize_resource`)는 DB 팀 파일과 같은 형식입니다: `json.dumps(indent=2, ensure_ascii=False) + "\n"`, 줄바꿈 CRLF, UTF-8 (BOM 없음).


| 항목 | 값 |
|---|---|
| 확인한 환경 | Python 3.13.13, pytest 9.2.0, macOS arm64 (`test_results/environment.txt`) |
| 대상 런타임 | Python 3.12 이상 (DB Lambda와 같은 3.12에서도 동작하는 문법만 사용) |
| 실행 의존성 | 없음 (표준 라이브러리) |
| 테스트 의존성 | `pytest>=8` |

설정은 환경 변수가 아니라 **함수 인자**로 줍니다.

```python
parse_bundle(
    bundle,                                   # FHIR R4 Bundle (dict)
    source="carecompass-demo-v1",             # sourceSystem. resourceKey 앞부분, DB patientToken.sourceSystems에 있어야 함
    encounter_info=info,                      # 진료과·병원·제목 매핑. None이면 진료과 UNKNOWN, 제목은 FHIR에서
    is_synthetic=True,                        # True → S3 prefix carecompass/v1/data/synthetic/fhir/
                                              # False → carecompass/v1/data/fhir/
    bundle_s3_key=None,                       # 원본 Bundle도 S3에 둘 경우 그 key (예: carecompass/v1/data/synthetic/fhir/bundles/A1.bundle.json)
)
```

## 4. 입력·출력 예시

- 입력: `examples/input_A1_small.bundle.json`. A1 환자의 Patient, 진료 1회(Encounter), 진단, 검사, 후속 계획, 예약, 병원(Organization·Location) 8건입니다.
- 출력: `examples/output_A1_small.json` (Records 6행, FollowUps 1행)

출력 Encounter 1행 (일부 필드):

```json
{
  "schemaVersion": 2, "itemType": "RESOURCE",
  "resourceKey": "carecompass-demo-v1|Encounter/A1-08",
  "patientKey": "A1", "resourceType": "Encounter", "fhirStatus": "finished",
  "s3Key": "carecompass/v1/data/synthetic/fhir/resources/Encounter-A1-08.json",
  "sourceSha256": "<64자 hex>",
  "title": "무릎 골관절염 정기 추적 진료 8회차",
  "occurredAt": "2026-10-15T00:00:00Z", "datePrecision": "DATETIME",
  "sourceDateValue": "2026-10-15T09:00:00+09:00", "sourceDatePrecision": "DATETIME",
  "dateEvidencePath": "Encounter.period.start",
  "specialtyCode": "05", "specialtySource": "DEMO_MAPPING",
  "specialtyEvidence": {"path": "scenario.visits.A1-08", "sourceValue": "정형외과",
                        "mappingVersion": "carecompass-demo-specialty-v1", "description": "scenarios.json:A1.visits:A1-08; ..."},
  "organizationName": "가톨릭정형외과의원", "organizationNameSource": "FHIR",
  "linkedResourceKeys": ["...|CarePlan/A1-careplan-01", "...|Condition/A1-condition-01", "...|Observation/A1-observation-08", "...|Patient/A1"],
  "linkageStatus": "RESOLVED", "unresolvedReferences": [],
  "linkEvidence": [{"targetResourceKey": "...|Condition/A1-condition-01", "reference": "Condition/A1-condition-01", "path": "Encounter.diagnosis[0].condition"}]
}
```

출력 FollowUp 1행:

```json
{
  "schemaVersion": 2, "patientKey": "A1", "kind": "APPOINTMENT", "title": "정형외과 재진 예약",
  "sourceResourceKey": "carecompass-demo-v1|Appointment/A1-appointment-01", "sourceElementKey": "$",
  "followupKey": "SHA-256(sourceResourceKey + \\n + $)",
  "relatedSourceResourceKeys": ["carecompass-demo-v1|CarePlan/A1-careplan-01"],
  "sourceStatus": "booked", "activityStatus": null,
  "scheduledAt": "2026-11-12T01:00:00Z", "scheduledDate": null, "datePrecision": "DATETIME",
  "timingEvidence": {"path": "Appointment.start", "originalValue": "2026-11-12T10:00:00+09:00"},
  "specialtyCode": "05", "clinicKey": "hira|...", "isSynthetic": true
}
```

## 5. DB 팀 6명·36회 샘플 변환 결과

입력은 `sample_data/carecompass_6patients/`(DB 팀 패키지 일부), 출력은 `output/v2/`입니다.

| 파일 | 내용 |
|---|---|
| `Records.jsonl` | 105행: Patient 6, Encounter 36, Condition 12, Observation 36, DiagnosticReport 8, MedicationRequest 1, Appointment 3, CarePlan 3 |
| `FollowUps.jsonl` | 4행: A1 예약(booked), B1 예약(cancelled), B1 날짜 미정 계획, C1 예약(booked) |
| `s3_objects.json` | S3 원문 122건의 `{s3Key: sourceSha256}` (Organization 8, Location 8, CodeSystem 1 포함). DB 팀 `manifest.json`의 파일 해시와 같음 |
| `upsert_batches/<환자>.json` | 환자별 `upsert_records` 요청 params 예시 (`expectedStateVersion`·`sourceVerification` 제외) |
| `errors.json` | 변환 실패 0건 |
| `summary.txt` | 환자별 진료 횟수·진료과·기간·후속 일정 |

- 모든 행이 **DB 팀 쓰기 Lambda 검증 코드를 통과**합니다.
- DB 팀 v1 결과(`query_models/Records.jsonl`·`FollowUps.jsonl`)와 resourceKey 집합, 상태, 진료과, 연결, 시각이 같습니다.

## 6. 테스트 코드·실행 방법·결과

- 코드: `tests/test_fhir_parser.py`
- 실행: `python3 -m pytest -q`
- 결과: **67 passed, 0 failed, 0 skipped** (`test_results/pytest_output.txt`, `test_results/junit.xml`)

| 묶음 | 확인 내용 |
|---|---|
| DB 계약 | 전 행이 `tests/db_contract/model_validation.py`(DB 팀 DB Lambda 1.1의 `src/`를 수정 없이 복사)의 `validate_record_model` / `validate_followup_model` 통과. 쓰기 트랜잭션 규칙(예약 날짜가 원본 Appointment와 같은지, 진료과 근거, 원본 상태 보존) 확인. 요청 묶음 순서, sha256과 S3 바이트 일치. **원문 해시가 DB 팀 `manifest.json`과 122건 모두 일치** |
| 골든 | DB 팀 v1 결과와 핵심 값 동일. 개수 105/4/122, 오류 0 |
| 안전 | 다른 환자 리소스는 연결하지 않음. 모든 연결에 근거 있음. 진료과 매핑이 없으면 UNKNOWN |
| 환자 간 혼입·누락 (갱신) | 다른 환자 CarePlan→Appointment 미연결·계획 유지·제목/진료과 미유출, 취소 예약에 묶인 계획 유지, 환자 2명 예약, 환자 없음, 같은 ID 다른 내용, fullUrl 충돌, DB가 거부할 값. **이전 버전에서는 이 중 12개가 실패함을 확인** |
| 단위 | 날짜 12가지(시간대·소수 초·날짜만·연월·연도·윤초·형식 오류), 끊긴 참조, 조건부 참조, 양방향 연결, 병원 이름, 실데이터 prefix, 한 건 실패 시 나머지 계속, 완료된 계획 제외, 날짜 없는 예약, 날짜만 있는 예약, 재파싱 동일·중복 제거 |

## 7. 적용한 계약 문서 버전

| 문서 | 버전·날짜 | 적용 범위 |
|---|---|---|
| DB Lambda 인터페이스 명세 | 전달본 1.1(2026-11-03) → **API v2(2026-11-06)** | §5.2 Record / §5.3 Followup query model **schemaVersion 2** (v2에서 변경 없음). 요청은 apiVersion `"2"` + `auth.accessToken` |
| DB Lambda 검증 코드 `src/model_validation.py` | **v2 전달본(2026-11-06, Cognito 인증)** 사본 (`tests/db_contract/`). 1.1과 Record/Followup 검증 규칙 동일 | 테스트 기준 |
| 백엔드–DB 계약 초안 | v1 | resourceKey 형식, 날짜 정밀도 보존, 같은 환자라도 근거 없이 묶지 않음, 재적재 중복 방지 |
| FHIR | R4 4.0.1 | 상태값 허용 집합 |

## 8. 날짜가 없을 때 처리 기준

원본 값과 정밀도를 그대로 보존하고, **날짜를 지어내지 않습니다.**

| 원본 값 | occurredAt | datePrecision | sourceDateValue / sourceDatePrecision |
|---|---|---|---|
| `2025-01-16T09:00:00+09:00` | `2025-01-16T00:00:00Z` (UTC, 소수 초 보존) | DATETIME | 원본 그대로 / DATETIME |
| `2025-01-16` | `2025-01-16` | DATE | `2025-01-16` / DATE |
| `2025-01-16T09:00:00` (시간대 없음) | `2025-01-16` | DATE | `2025-01-16` / DATE. **시간대를 추정하지 않고 날짜로 낮춥니다** |
| `2025-01` | null | UNKNOWN | `2025-01` / MONTH |
| `2025` | null | UNKNOWN | `2025` / YEAR |
| 없음, 형식 오류, 없는 날짜(2/30), 윤초(:60) | null | UNKNOWN | null / UNKNOWN |

- 리소스별로 사용하는 날짜 필드:
  - Encounter: `period.start`
  - Observation·DiagnosticReport: `effectiveDateTime` → `effectivePeriod.start` → `issued`
  - Condition: `onsetDateTime` → `recordedDate`
  - MedicationRequest: `authoredOn`
  - Appointment: `start`
  - 사용한 경로는 `dateEvidencePath`에 기록합니다.
- Patient와 CarePlan은 발생 시점이 없는 것으로 봅니다(UNKNOWN). CarePlan의 `period`를 예약일로 쓰지 않습니다.
- FollowUps:
  - 예약: 원본 Appointment 레코드의 날짜 모델을 그대로 씁니다. DATETIME이면 `scheduledAt`, DATE이면 `scheduledDate`, UNKNOWN이면 둘 다 null.
  - 날짜 없는 계획: 둘 다 null이고 `timingEvidence`도 null입니다.

## 9. 참조 연결에 실패했을 때 처리 기준

| 경우 | 처리 |
|---|---|
| 번들 안에서 찾은 **같은 환자** 리소스 | `linkedResourceKeys`에 추가하고 `linkEvidence{targetResourceKey, reference, path}`에 근거를 남김 |
| 그 Encounter를 가리키는 같은 환자 리소스 | Encounter 쪽에도 연결 (근거 경로에 `(reverse)`) |
| 번들에서 **찾을 수 없는** 참조 | `unresolvedReferences`에 원문 문자열로 남기고 `linkageStatus=PARTIAL`. 연결 목록에는 넣지 않음 |
| **다른 환자**의 리소스 | 연결하지 않음. 실패로도 세지 않음 |
| 조건부 참조(`Organization?identifier=...`) | 번들 밖을 가리키므로 무시 |
| Organization·Location·CodeSystem | 연결 대신 `sharedReferences{resourceKey, resourceType, s3Key}` |
| 환자를 알 수 없는 리소스 | Records 행을 만들지 않음 (원문은 S3 목록에 포함) |
| 한 리소스 변환 중 오류 | 그 건만 `errors`에 기록하고 나머지는 계속 변환 |

**갱신 (2026-11-06)**: 모호하면 어느 환자에게도 붙이지 않고 `errors`에 남깁니다.

| 경우 | 처리 | errors code |
|---|---|---|
| CarePlan이 **다른 환자**의 Appointment를 가리킴 | 예약에 묶지 않음(제목·진료과·참조 미유출). 계획은 자기 환자의 `CARE_PLAN` 행으로 남음 | (오류 아님) |
| CarePlan이 **취소·노쇼·오입력** 예약을 가리킴 | 예약에 묶지 않고 계획 행을 따로 남김 ('다음 진료'에서 계획이 사라지지 않게) | (오류 아님) |
| 한 리소스에 환자가 2명 이상 (예: 예약 참여자 2명) | 변환하지 않음 | `AMBIGUOUS_OWNER` |
| 환자 참조 중 **하나라도 확인되지 않음** (번들에 없는 환자, 종류 표시 불일치, 빈 참조) | 변환하지 않음. 번들에 없는 의사 등 환자가 아닌 게 분명한 참조는 무시 | `OWNER_UNRESOLVED` |
| 번들에서 환자를 찾을 수 없음 | 변환하지 않음 (예전엔 조용히 빠졌음) | `OWNER_UNRESOLVED` |
| 같은 Type/id에 서로 다른 내용 | 둘 다 변환하지 않음 (같은 내용 중복은 정상) | `DUPLICATE_RESOURCE_ID` |
| 같은 fullUrl이 서로 다른 리소스 | 그 주소로는 참조를 풀지 않음 | `DUPLICATE_FULL_URL` |
| DB가 거부할 값 (FHIR id 형식, R4 상태값) | 그 행만 빼고 기록. DB는 한 행만 틀려도 요청 전체(최대 50행)를 거부하기 때문 | `INVALID_FOR_DB` |
| 변환 중 예외 | 그 건만 빼고 계속 | `PARSE_ERROR` |

참고: DB 쓰기 Lambda는 저장할 때 `linkageStatus`를 다시 계산합니다. 연결 대상 행이 아직 DB에 없으면 PARTIAL로 저장됩니다. 조회 `get_record_detail`은 읽을 때 또 한 번 계산합니다.

## 10. S3 저장·DB 적재 포함 여부

**포함되어 있지 않습니다.** 파서가 만드는 것은 아래 세 가지입니다.

1. 넣을 행: `records`, `followups`
2. S3에 올릴 원문 바이트와 그 SHA-256: `object_bytes()`, `sourceSha256`
3. 요청 묶음: `upsert_batches()`

적재 순서로 남은 단계는 이렇습니다 (DB 팀 README 4절):

1. FHIR R4 공식 validator로 원본 검증
2. `object_bytes()`의 바이트를 그대로 S3에 PutObject → 다시 읽어서 SHA-256 확인
3. 위 두 단계가 끝난 뒤에만 `sourceVerification={fhirVersion:"4.0.1", validated:true, objectsVerified:true}`
4. `get_write_state` → `stateVersion`
5. 묶음마다 `upsert_records`. 요청마다 UUID `requestId`, 재시도할 때는 같은 UUID와 같은 payload

S3 저장과 적재 호출을 누가 맡을지 정해야 합니다. 정해지면 맞춰서 진행하겠습니다.

## 11. 남은 작업 / 알려진 제한

1. **S3 업로드와 `upsert_records` 호출 코드가 없습니다** (10절).
2. **FHIR 공식 validator 실행이 파서에 없습니다.** 파서는 형식과 DB 계약만 맞춥니다.
3. **원문 해시는 DB 팀 샘플 파일과 같습니다.** 원문 직렬화를 DB 팀 `build_package.py`와 같은 형식(들여쓰기 2칸 JSON, CRLF 줄바꿈, UTF-8)으로 맞춰, `sourceSha256`이 `manifest.json` 해시와 122건 모두 같습니다. 이미 적재된 시연 데이터는 그대로 두고 다시 적재하지 않는 것을 권장합니다.
4. 여러 요청으로 나눠 적재하면 먼저 들어간 행이 PARTIAL로 저장될 수 있습니다 (9절 참고).
5. 진료과는 시나리오 매핑(`DEMO_MAPPING`)만 지원합니다. 실제 병원 데이터의 진료과 출처(`SOURCE_MAPPING`)는 정해지지 않았습니다. 진료과 표시명은 01·05·14만 있습니다.
6. CarePlan은 1건을 1행으로 만듭니다. activity별 일정은 나누지 않습니다.
7. 예약을 `activity`로 가리키는 CarePlan은 예약 행에 묶이고 별도 행이 없습니다. 다른 방식이 필요하면 맞추겠습니다.
8. 의료진 진료 입력으로 기록을 추가하는 흐름은 범위 밖입니다.
9. **patientKey는 FHIR Patient id를 그대로 씁니다.** 샘플은 문제없지만, 실제로 여러 병원 데이터를 합치면 서로 다른 사람이 같은 id를 가질 수 있습니다. 실데이터에서는 계정 연결(UserPatientLinks)의 patientKey로 바꿔 넣는 단계가 필요합니다.
10. `INVALID_FOR_DB`로 빠진 행을 다른 행이 가리키면, 그 행은 DB에서 PARTIAL로 저장됩니다. Patient 행이 빠지면 그 환자의 다른 행은 DB가 받지 않습니다.
