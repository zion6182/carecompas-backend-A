# CareCompass 백엔드 A: 의료기록·의료 여정 데이터 변환

CareCompass 백엔드에서 **의료기록·의료 여정 데이터 변환**을 맡은 부분의 산출물입니다.
병원에서 받은 표준 **FHIR R4** 의료데이터를 서비스 화면에서 조회할 수 있는 진료기록(Records)과 후속진료 일정(FollowUps) 데이터로 바꿉니다.

## 구현 내용

- **FHIR 파서** (`src/fhir_parser.py`, `src/specialty_map.py`)
  - 진료(Encounter)를 중심으로 진단·검사·처방 리소스를 참조로 연결해 진료기록 행을 만듭니다.
  - 예약(Appointment)과 진료 계획(CarePlan)을 후속진료 일정으로 정리하고, 취소된 예약과 날짜 미정 계획을 구분합니다.
  - 출력은 DB 팀과 합의한 조회 모델 **schemaVersion 2** 형식입니다.
  - 날짜는 원본 정밀도를 보존하고, 시간대가 없거나 형식이 틀린 값을 추정하지 않습니다.
  - 다른 환자의 리소스는 연결하지 않습니다. 소유 환자가 불분명하거나 같은 ID에 내용이 다른 리소스는 변환하지 않고 오류로 기록합니다.
  - 원문 직렬화를 DB 팀과 맞춰 FHIR 원문 122건의 SHA-256이 DB 팀 manifest와 모두 일치합니다.
  - Python 표준 라이브러리만 사용합니다.
- **S3 저장·DB 적재 코드** (`src/ingest.py`, `src/s3_store.py`, `src/db_writer.py`, `src/fhir_validate.py`)
  - 허용된 S3 경로에만 저장하고, 다른 내용의 기존 파일은 덮어쓰지 않으며, 업로드 후 다시 읽어 해시를 확인합니다.
  - DB 쓰기 Lambda(`apiVersion "2"`)로 50행 단위 적재. 통신 오류 시 같은 requestId로 재시도, 상태 충돌 시 최신 상태로 재요청합니다.
  - ⚠️ **가짜 AWS(`tests/fake_aws.py`)로만 테스트했고, 실제 AWS에는 연결하지 않았습니다.** FHIR 공식 validator 실행도 연결되어 있지 않습니다.

## 실행

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m pytest -q                      # 테스트 100개 (파서 67, 적재 33)
python3 scripts/export_query_models.py    # 합성 환자 6명 → output/v2/
python3 scripts/summarize_samples.py      # 환자별 요약
python3 scripts/ingest_dry_run.py         # 적재 미리보기 (AWS 호출 없음)
```

## 결과 (합성 환자 6명, 진료 36회)

| 항목 | 값 |
|---|---|
| Records | 105행 |
| FollowUps | 4행 (예약 2, 취소 예약 1, 날짜 미정 계획 1) |
| S3 원문 | 122건, DB 팀 manifest 해시와 모두 일치 |
| 변환 오류 | 0건 |

## 폴더

```
src/            파서와 적재 코드
tests/          테스트. db_contract/는 DB 팀 쓰기 Lambda 검증 코드 사본
scripts/        변환·요약·적재 미리보기 스크립트
sample_data/    DB 팀 합성 데이터 일부 (실제 환자 정보 아님)
output/v2/      변환 결과
examples/       작은 입력·출력 예시
docs/           전달 문서(HANDOFF.md)
```

## 남은 작업

- 실제 AWS S3·DB 적재 연결 (테스트 환자 계정 연결, 실행 권한, 토큰 발급 방법 필요)
- FHIR 공식 validator 연결
- 의료진 진료 입력을 후속진료 일정에 반영
